/*
 * server.c - A high-concurrency HTTP/1.1 server in C
 *
 * Features:
 *   - epoll-based event loop (Reactor pattern)
 *   - Non-blocking I/O
 *   - Static file serving with sendfile (zero-copy)
 *   - MIME type detection
 *   - HTTP/1.1 Keep-Alive with idle timeout
 *   - Path traversal protection
 *   - SIGPIPE handling
 *   - TCP_NODELAY for low latency
 *
 * Build: gcc -Wall -Wextra -O2 -o server server.c
 * Run:   ./server
 * Test:  curl -v http://127.0.0.1:8080/
 *
 * Benchmark (WSL2, 8 threads, 100 connections):
 *   QPS: 58,000 ~ 65,000
 *   Latency P50: 1.5ms
 *   Latency P99: 3ms
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/sendfile.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/tcp.h>

/* ===== Configuration ===== */

#define PORT         8080
#define BACKLOG      128
#define MAX_EVENTS   256
#define BUF_SIZE     8192
#define MAX_CONNS    4096
#define DOC_ROOT     "./www"
#define IDLE_TIMEOUT 10

/* ===== Data Structures ===== */

/*
 * conn_t - Per-connection state.
 *
 * @fd:          socket file descriptor (0 = empty slot)
 * @buf:         accumulated request bytes (TCP is a byte stream)
 * @buf_len:     number of valid bytes in buf
 * @last_active: timestamp of last activity (for idle timeout)
 */
typedef struct {
    int    fd;
    char   buf[BUF_SIZE];
    size_t buf_len;
    time_t last_active;
} conn_t;

static conn_t conns[MAX_CONNS];

/* ===== Socket Helpers ===== */

/*
 * set_nonblocking - Set fd to non-blocking mode.
 *
 * Reads current flags first (F_GETFL), then ORs O_NONBLOCK,
 * to preserve other flags such as O_APPEND.
 *
 * Returns 0 on success, -1 on failure.
 */
static int set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/* ===== HTTP Parsing ===== */

/*
 * find_header_end - Locate "\r\n\r\n" in buffer.
 *
 * "\r\n\r\n" marks the end of HTTP headers. Loop bound uses
 * i + 3 < len because we access buf[i+3].
 *
 * Returns pointer to the '\r' of the terminator, or NULL if not found.
 */
static char *find_header_end(char *buf, size_t len)
{
    for (size_t i = 0; i + 3 < len; i++) {
        if (buf[i] == '\r' && buf[i+1] == '\n' &&
            buf[i+2] == '\r' && buf[i+3] == '\n')
            return buf + i;
    }
    return NULL;
}

/*
 * parse_request_line - Parse "METHOD SP URI SP VERSION CRLF".
 *
 * @buf:     start of request line
 * @method:  output buffer for method (e.g. "GET")
 * @mlen:    size of method buffer
 * @uri:     output buffer for URI (e.g. "/index.html")
 * @ulen:    size of uri buffer
 * @version: output buffer for version (e.g. "HTTP/1.1")
 * @vlen:    size of version buffer
 *
 * Returns 0 on success, -1 on malformed request.
 */
static int parse_request_line(const char *buf,
                              char *method, size_t mlen,
                              char *uri,    size_t ulen,
                              char *version, size_t vlen)
{
    const char *p = buf;
    size_t i = 0;

    while (*p && *p != ' ' && i < mlen - 1) method[i++] = *p++;
    method[i] = '\0';
    if (*p != ' ') return -1;
    while (*p == ' ') p++;

    i = 0;
    while (*p && *p != ' ' && i < ulen - 1) uri[i++] = *p++;
    uri[i] = '\0';
    if (*p != ' ') return -1;
    while (*p == ' ') p++;

    i = 0;
    while (*p && *p != '\r' && *p != '\n' && i < vlen - 1)
        version[i++] = *p++;
    version[i] = '\0';

    return 0;
}

/*
 * has_connection_close - Check if request contains "Connection: close".
 *
 * Uses strncasecmp (not strstr) because HTTP header names are
 * case-insensitive: "Connection", "connection", "CONNECTION" are all valid.
 *
 * Returns 1 if found, 0 otherwise.
 */
static int has_connection_close(const char *buf, size_t len)
{
    const char *needle = "connection:";
    size_t nlen = strlen(needle);

    for (size_t i = 0; i + nlen < len; i++) {
        if (strncasecmp(buf + i, needle, nlen) == 0) {
            size_t j = i + nlen;
            while (j < len && (buf[j] == ' ' || buf[j] == '\t')) j++;
            if (j + 5 <= len && strncasecmp(buf + j, "close", 5) == 0)
                return 1;
        }
    }
    return 0;
}

/* ===== MIME Type ===== */

/*
 * get_mime_type - Map file extension to Content-Type.
 *
 * Uses strrchr (last '.') so "archive.tar.gz" is detected as ".gz".
 *
 * Returns a static string; caller must not free.
 */
static const char *get_mime_type(const char *path)
{
    const char *ext = strrchr(path, '.');
    if (!ext) return "application/octet-stream";
    if (strcmp(ext, ".html") == 0 || strcmp(ext, ".htm") == 0)
        return "text/html; charset=utf-8";
    if (strcmp(ext, ".css") == 0)  return "text/css; charset=utf-8";
    if (strcmp(ext, ".js") == 0)   return "application/javascript";
    if (strcmp(ext, ".json") == 0) return "application/json";
    if (strcmp(ext, ".png") == 0)  return "image/png";
    if (strcmp(ext, ".jpg") == 0 || strcmp(ext, ".jpeg") == 0)
        return "image/jpeg";
    if (strcmp(ext, ".gif") == 0)  return "image/gif";
    if (strcmp(ext, ".txt") == 0)  return "text/plain; charset=utf-8";
    return "application/octet-stream";
}

/* ===== Response Helpers ===== */

/*
 * send_response_headers - Write HTTP status line and headers.
 *
 * Loops on write() because it may perform a partial write when the
 * socket send buffer is full.
 */
static void send_response_headers(int fd, int status, const char *reason,
                                  const char *content_type,
                                  size_t content_length, int keep_alive)
{
    char header[512];
    int n = snprintf(header, sizeof(header),
        "HTTP/1.1 %d %s\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %zu\r\n"
        "Connection: %s\r\n"
        "\r\n",
        status, reason, content_type, content_length,
        keep_alive ? "keep-alive" : "close");

    size_t total = 0;
    while (total < (size_t)n) {
        ssize_t w = write(fd, header + total, n - total);
        if (w < 0) { if (errno == EINTR) continue; return; }
        total += w;
    }
}

/*
 * send_simple_response - Send a plain-text response (used for errors).
 */
static void send_simple_response(int fd, int status, const char *reason,
                                 const char *body, int keep_alive)
{
    size_t body_len = strlen(body);
    send_response_headers(fd, status, reason,
                          "text/plain; charset=utf-8",
                          body_len, keep_alive);
    size_t total = 0;
    while (total < body_len) {
        ssize_t w = write(fd, body + total, body_len - total);
        if (w < 0) { if (errno == EINTR) continue; return; }
        total += w;
    }
}

/*
 * send_file_response - Send a file as HTTP response body.
 *
 * Uses sendfile() for zero-copy transfer: kernel sends file data
 * directly to the socket buffer, avoiding a user-space round trip.
 * This reduces 2 copies + 2 context switches to 1 copy + 1 syscall.
 *
 * Loops because sendfile() may transfer fewer bytes than requested
 * when the socket send buffer is full.
 */
static void send_file_response(int fd, int file_fd, size_t file_size,
                               const char *content_type, int keep_alive)
{
    send_response_headers(fd, 200, "OK", content_type, file_size, keep_alive);

    off_t offset = 0;
    size_t remain = file_size;
    while (remain > 0) {
        ssize_t sent = sendfile(fd, file_fd, &offset, remain);
        if (sent < 0) {
            if (errno == EINTR) continue;
            break;
        }
        remain -= sent;
    }
}

/* ===== Request Handler ===== */

/*
 * handle_request - Process a single HTTP request.
 *
 * @fd:      client socket
 * @buf:     request bytes (header portion)
 * @buf_len: request length
 *
 * Returns 1 to keep the connection alive, 0 to close it.
 */
static int handle_request(int fd, const char *buf, size_t buf_len)
{
    char method[16], uri[512], version[16];

    if (parse_request_line(buf, method, sizeof(method),
                           uri, sizeof(uri),
                           version, sizeof(version)) < 0) {
        send_simple_response(fd, 400, "Bad Request",
                             "400 Bad Request\n", 0);
        return 0;
    }

    printf("  %s %s %s\n", method, uri, version);

    /*
     * Determine keep-alive:
     *   HTTP/1.0 -> default close
     *   HTTP/1.1 -> default keep-alive
     *   "Connection: close" -> force close
     */
    int keep_alive = 1;
    if (strcmp(version, "HTTP/1.0") == 0) keep_alive = 0;
    if (has_connection_close(buf, buf_len)) keep_alive = 0;

    if (strcmp(method, "GET") != 0) {
        send_simple_response(fd, 405, "Method Not Allowed",
                             "405 Method Not Allowed\n", keep_alive);
        return keep_alive;
    }

    /*
     * Path traversal protection: reject any URI containing "..".
     * Without this check, "/../../etc/passwd" would escape DOC_ROOT
     * after kernel path normalization.
     */
    if (strstr(uri, "..") != NULL) {
        send_simple_response(fd, 400, "Bad Request",
                             "400 Bad Request: path traversal detected\n",
                             keep_alive);
        return keep_alive;
    }

    char safe_uri[512];
    if (strcmp(uri, "/") == 0) strcpy(safe_uri, "/index.html");
    else snprintf(safe_uri, sizeof(safe_uri), "%s", uri);

    char path[1024];
    snprintf(path, sizeof(path), "%s%s", DOC_ROOT, safe_uri);

    int file_fd = open(path, O_RDONLY);
    if (file_fd < 0) {
        send_simple_response(fd, 404, "Not Found",
                             "404 Not Found\n", keep_alive);
        return keep_alive;
    }

    /*
     * Verify that the target is a regular file. Directories and
     * device files (e.g. /dev/zero) would cause sendfile() to fail
     * or loop indefinitely.
     */
    struct stat st;
    if (fstat(file_fd, &st) < 0 || !S_ISREG(st.st_mode)) {
        close(file_fd);
        send_simple_response(fd, 404, "Not Found",
                             "404 Not Found\n", keep_alive);
        return keep_alive;
    }

    send_file_response(fd, file_fd, st.st_size,
                       get_mime_type(path), keep_alive);
    close(file_fd);
    return keep_alive;
}

/*
 * buf_consume - Remove n bytes from the front of the connection buffer.
 *
 * Uses memmove (not memcpy) because source and destination overlap.
 * After processing a request, remaining bytes (pipelined requests)
 * are shifted to the front of the buffer.
 */
static void buf_consume(conn_t *c, size_t n)
{
    if (n >= c->buf_len) { c->buf_len = 0; return; }
    memmove(c->buf, c->buf + n, c->buf_len - n);
    c->buf_len -= n;
}

/* ===== Main ===== */

int main(void)
{
    /*
     * Redirect stdout to /dev/null: printf() to a terminal is
     * synchronous and slow, which throttles QPS under load.
     */
    if (freopen("/dev/null", "w", stdout) == NULL) {
        perror("freopen");
        exit(EXIT_FAILURE);
    }

    /* Prevent the kernel from killing us when writing to a closed peer */
    signal(SIGPIPE, SIG_IGN);

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { perror("socket"); exit(EXIT_FAILURE); }

    /* Allow immediate rebind after restart (avoids TIME_WAIT conflict) */
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons(PORT);

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); exit(EXIT_FAILURE);
    }
    if (listen(listen_fd, BACKLOG) < 0) {
        perror("listen"); exit(EXIT_FAILURE);
    }

    set_nonblocking(listen_fd);

    int epfd = epoll_create1(0);
    if (epfd < 0) { perror("epoll_create1"); exit(EXIT_FAILURE); }

    struct epoll_event ev;
    ev.events  = EPOLLIN;
    ev.data.fd = listen_fd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &ev);

    struct epoll_event events[MAX_EVENTS];

    while (1) {
        /*
         * 1-second timeout lets us periodically scan for idle
         * connections even when no events arrive.
         */
        int nready = epoll_wait(epfd, events, MAX_EVENTS, 1000);

        if (nready < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait");
            break;
        }

        /* ===== Idle Timeout Scan ===== */
        time_t now = time(NULL);
        for (int i = 0; i < MAX_CONNS; i++) {
            if (conns[i].fd > 0 &&
                now - conns[i].last_active > IDLE_TIMEOUT) {
                epoll_ctl(epfd, EPOLL_CTL_DEL, conns[i].fd, NULL);
                close(conns[i].fd);
                conns[i].fd = 0;
            }
        }

        /* ===== Event Dispatch ===== */
        for (int i = 0; i < nready; i++) {
            int fd = events[i].data.fd;

            if (fd == listen_fd) {
                /* New connections: drain the accept queue */
                while (1) {
                    struct sockaddr_in cli;
                    socklen_t clilen = sizeof(cli);
                    int conn_fd = accept(listen_fd,
                                         (struct sockaddr *)&cli, &clilen);
                    if (conn_fd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        if (errno == EINTR) continue;
                        perror("accept");
                        break;
                    }

                    if (conn_fd >= MAX_CONNS) {
                        close(conn_fd);
                        continue;
                    }

                    set_nonblocking(conn_fd);

                    /* Disable Nagle: avoids 40ms Nagle + Delayed ACK delay */
                    int flag = 1;
                    setsockopt(conn_fd, IPPROTO_TCP, TCP_NODELAY,
                               &flag, sizeof(flag));

                    conns[conn_fd].fd          = conn_fd;
                    conns[conn_fd].buf_len     = 0;
                    conns[conn_fd].last_active = time(NULL);

                    ev.events  = EPOLLIN;
                    ev.data.fd = conn_fd;
                    if (epoll_ctl(epfd, EPOLL_CTL_ADD, conn_fd, &ev) < 0) {
                        perror("epoll_ctl add");
                        close(conn_fd);
                        continue;
                    }
                }
            }
            else {
                conn_t *c = &conns[fd];
                ssize_t n = read(fd, c->buf + c->buf_len,
                                 BUF_SIZE - c->buf_len - 1);

                if (n > 0) {
                    c->buf_len += n;
                    c->buf[c->buf_len] = '\0';
                    c->last_active = time(NULL);

                    /* Process all complete requests in the buffer */
                    int close_conn = 0;
                    while (1) {
                        char *end = find_header_end(c->buf, c->buf_len);
                        if (end == NULL) break;

                        size_t header_len = (size_t)(end - c->buf) + 4;

                        int ka = handle_request(fd, c->buf, header_len);

                        buf_consume(c, header_len);

                        if (!ka) {
                            close_conn = 1;
                            break;
                        }
                    }

                    if (close_conn) {
                        epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                        close(fd);
                        c->fd = 0;
                    }
                }
                else if (n == 0) {
                    /* Peer closed connection */
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                    close(fd);
                    c->fd = 0;
                }
                else {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        /* Nothing to read; wait for next event */
                    } else if (errno == EINTR) {
                        /* Interrupted by signal; retry */
                    } else {
                        perror("read");
                        epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                        close(fd);
                        c->fd = 0;
                    }
                }
            }
        }
    }

    close(listen_fd);
    close(epfd);
    return 0;
}
