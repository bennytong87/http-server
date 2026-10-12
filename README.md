# HTTP Server in C

A high-concurrency HTTP/1.1 server implemented in C using epoll.

## Features

- epoll-based event loop (Reactor pattern)
- Non-blocking I/O
- Static file serving with sendfile (zero-copy)
- MIME type detection
- HTTP/1.1 Keep-Alive with idle timeout
- Path traversal protection
- SIGPIPE handling
- TCP_NODELAY for low latency

## Build

    make

## Run

    mkdir -p www
    echo '<h1>Hello</h1>' > www/index.html
    ./server

## Test

    curl -v http://127.0.0.1:8080/

## Benchmark

Tested on WSL2 with: wrk -t8 -c100 -d30s --latency

| Metric        | Value           |
|---------------|-----------------|
| QPS           | 58,000 ~ 65,000 |
| Latency P50   | 1.5ms           |
| Latency P99   | 3ms             |
| Transfer/sec  | 7.5 ~ 8.4MB     |

Note: WSL2 performance varies due to virtualization scheduling.
Native Linux should be more stable.

## Architecture

    main()
      |
      +-- socket() -> bind() -> listen() -> epoll_create1()
      |
      +-- Event Loop
            |
            +-- epoll_wait()
                  |
                  +-- if fd == listen_fd: accept()
                  |
                  +-- else: read() / write() / sendfile()

## Key Design Decisions

1. epoll instead of select: O(1) event retrieval, no fd limit.
2. Non-blocking I/O: Avoid blocking on accept/read/write.
3. sendfile: Zero-copy file transfer.
4. TCP_NODELAY: Avoids Nagle + Delayed ACK 40ms delay.
5. Keep-Alive: Reuses TCP connections.
6. Idle timeout: Prevents slowloris attack.
7. Path traversal protection: Rejects URIs containing "..".

## Known Limitations

- Single-threaded event loop (only one CPU core utilized).
- No HTTPS support.
- No POST/PUT/DELETE methods.

