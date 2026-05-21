# Simple C/S Architecture Project

A high-performance chat server/client system implemented in C language, using epoll multi-threaded architecture to support high-concurrency scenarios.

## Project Structure

```
.
├── server.c          # Server main program
├── client.c          # Client program
├── bench_test.c      # Stress testing tool
├── CMakeLists.txt    # CMake build configuration
└── build/            # Build output directory
```

## Core Design

### Server (server.c)

**Architecture Features:**
- Multi-threaded Reactor pattern with epoll for I/O multiplexing
- Default 4 worker threads, supporting up to 20,000 concurrent connections
- Ring buffer management for data read/write
- Socketpair for communication between main thread and worker threads

**Data Structures:**
- `ringbuf_t` - Ring buffer for efficient network I/O management
- `client_t` - Client state management (file descriptor, state machine, read/write buffers)
- `worker_ctx_t` - Worker thread context (epoll instance, client list)
- `user_t` - User information (username, password, online status, group list)
- `group_t` - Group information (member list, creator)

**Features:**
- User registration and login
- Private messaging
- Group creation, joining, and broadcasting
- Online user list query
- Connection timeout detection (300 seconds)

**Configuration Parameters:**
- `PORT` - Server port (default 12345)
- `MAX_EVENTS` - Maximum epoll events (1024)
- `MAX_CLIENTS` - Maximum clients (20,000)
- `THREAD_COUNT` - Worker threads (4)
- `BUFFER_SIZE` - Buffer size (4096 bytes)

### Client (client.c)

**Features:**
- Non-blocking I/O + select multiplexing
- Auto-reconnect mechanism
- Interactive command-line interface
- TCP_NODELAY for low latency optimization

**Supported Commands:**
- `r <username> <password>` - Register new account
- `l <username> <password>` - Login
- `p <user> <message>` - Send private message
- `gc <groupname>` - Create group
- `gj <groupname>` - Join group
- `gm <groupname> <message>` - Group broadcast
- `gl` - List joined groups
- `ul` - List online users
- `logout` - Logout
- `quit/exit` - Exit

### Stress Testing Tool (bench_test.c)

**Test Modes:**
- `conn` - Connection storm: Test TCP connection establishment rate
- `login` - Login storm: Test user registration/login throughput
- `ping` - RTT latency test: Measure round-trip delay distribution
- `msg` - Message exchange test: Test private message throughput
- `stress` - Mixed stress test: Connection → Login → Message → Exit → Reconnect cycle

**Usage Example:**
```bash
./bench_test -s 127.0.0.1 -p 12345 -t stress -n 5000 -c 200 -d 60
```

**Parameter Description:**
- `-s` - Server address
- `-p` - Server port
- `-t` - Test type
- `-n` - Total connections
- `-c` - Concurrent connections
- `-d` - Test duration (seconds)

## Compilation and Running

### Compilation

```bash
mkdir build && cd build
cmake ..
make
```

### Run Server

```bash
./server
```

### Run Client

```bash
./client
```

### Run Stress Test

```bash
./bench_test -s 127.0.0.1 -p 12345 -t ping -n 1000 -c 50 -d 30
```

## Technology Stack

- **Language**: C99
- **Build System**: CMake 3.10+
- **Network**: Linux epoll, POSIX sockets
- **Concurrency**: pthreads
- **Compilation Optimization**: -O2

## Dependencies

- Linux operating system (epoll dependency)
- GCC compiler
- CMake 3.10+
- pthread library

## Performance Metrics

Server design targets:
- Support 20,000 concurrent connections
- 4 worker threads for parallel processing
- Maximum 5,500 connections per worker
- 300-second connection timeout detection

## License

This project is for learning and research purposes only.