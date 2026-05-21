#include <arpa/inet.h>
#include <assert.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <pwd.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>

#define PORT 12345
#define STATS_PORT 12346
#define MAX_EVENTS 1024
#define MAX_CLIENTS 20000
#define MAX_USERS 20000
#define MAX_GROUPS 100
#define MAX_GROUPS_PER_USER 10
#define BUFFER_SIZE 4096
#define USERNAME_MAX 32
#define PASSWORD_MAX 32
#define GROUPNAME_MAX 32
#define MSG_MAX 1024
#define TIMEOUT_SECONDS 300
#define THREAD_COUNT 4
#define MAX_CLIENTS_PER_WORKER 5500

typedef enum { STATE_CONNECTED, STATE_LOGGED_IN, STATE_DISCONNECTED } client_state_t;

// 环形缓冲区
typedef struct ringbuf {
    char *buf;
    size_t size;
    size_t read_idx;
    size_t write_idx;
} ringbuf_t;

// 客户端结构
typedef struct client {
    int fd;
    client_state_t state;
    int user_idx;
    ringbuf_t read_ring;
    ringbuf_t write_ring;
    time_t last_active;
    time_t connect_time;
    int write_pending;  // 标记EPOLLOUT
} client_t;

// 工作线程上下文
typedef struct worker_ctx {
    int epoll_fd;
    int thread_id;
    pthread_t thread;
    int notify_fd_recv;  // socketpair 接收端（工作线程）
    int notify_fd_send;  // socketpair 发送端（主线程）
    client_t clients[MAX_CLIENTS];
    atomic_int client_count;
    pthread_mutex_t clients_lock;
} worker_ctx_t;

// 用户结构
typedef struct {
    char username[USERNAME_MAX];
    char password[PASSWORD_MAX];
    int online;
    int fd;
    int worker_id;
    int groups[MAX_GROUPS_PER_USER];
    int group_count;
} user_t;

// 群组结构
typedef struct {
    char name[GROUPNAME_MAX];
    int members[MAX_USERS];
    int member_count;
    int creator_idx;
} group_t;

static user_t users[MAX_USERS];
static int user_count = 0;
static group_t groups[MAX_GROUPS];
static int group_count = 0;
static int listen_fd = -1;
static int stats_fd = -1;
static volatile int running = 1;
static time_t server_start_time;
static unsigned long long total_messages = 0;
static unsigned long long total_bytes_recv = 0;
static unsigned long long total_bytes_sent = 0;

static pthread_rwlock_t user_lock = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t group_lock = PTHREAD_RWLOCK_INITIALIZER;
static pthread_mutex_t stats_lock = PTHREAD_MUTEX_INITIALIZER;

static worker_ctx_t workers[THREAD_COUNT];

// 信号处理
static void signal_handler(int sig) { running = 0; }

// ============ 环形缓冲区实现 ============
static void ringbuf_init(ringbuf_t *rb, size_t size) {
    rb->buf = malloc(size);
    rb->size = size;
    rb->read_idx = 0;
    rb->write_idx = 0;
}

static void ringbuf_free(ringbuf_t *rb) {
    if (rb->buf) free(rb->buf);
    rb->buf = NULL;
    rb->size = 0;
    rb->read_idx = 0;
    rb->write_idx = 0;
}

static size_t ringbuf_readable(ringbuf_t *rb) {
    if (rb->write_idx >= rb->read_idx)
        return rb->write_idx - rb->read_idx;
    else
        return rb->size - (rb->read_idx - rb->write_idx);
}

static size_t ringbuf_writable(ringbuf_t *rb) { return rb->size - 1 - ringbuf_readable(rb); }

static size_t ringbuf_write(ringbuf_t *rb, const char *data, size_t len) {
    size_t writable = ringbuf_writable(rb);
    if (writable == 0) return 0;
    if (len > writable) len = writable;
    size_t first_chunk = rb->size - rb->write_idx;
    if (first_chunk > len) first_chunk = len;
    memcpy(rb->buf + rb->write_idx, data, first_chunk);
    if (first_chunk < len) memcpy(rb->buf, data + first_chunk, len - first_chunk);
    rb->write_idx = (rb->write_idx + len) % rb->size;
    return len;
}

static size_t ringbuf_read(ringbuf_t *rb, char *out, size_t len) {
    size_t readable = ringbuf_readable(rb);
    if (readable == 0) return 0;
    if (len > readable) len = readable;
    size_t first_chunk = rb->size - rb->read_idx;
    if (first_chunk > len) first_chunk = len;
    memcpy(out, rb->buf + rb->read_idx, first_chunk);
    if (first_chunk < len) memcpy(out + first_chunk, rb->buf, len - first_chunk);
    rb->read_idx = (rb->read_idx + len) % rb->size;
    return len;
}

static size_t ringbuf_peek(ringbuf_t *rb, char *out, size_t len) {
    size_t readable = ringbuf_readable(rb);
    if (readable == 0) return 0;
    if (len > readable) len = readable;
    size_t first_chunk = rb->size - rb->read_idx;
    if (first_chunk > len) first_chunk = len;
    memcpy(out, rb->buf + rb->read_idx, first_chunk);
    if (first_chunk < len) memcpy(out + first_chunk, rb->buf, len - first_chunk);
    return len;
}

static ssize_t ringbuf_find_newline(ringbuf_t *rb) {
    size_t readable = ringbuf_readable(rb);
    for (size_t i = 0; i < readable; i++) {
        size_t idx = (rb->read_idx + i) % rb->size;
        if (rb->buf[idx] == '\n') return (ssize_t)i;
    }
    return -1;
}

// 工具函数
static void raise_fd_limit(void) {
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur < 65536) {
        rlim_t new_cur = 65536;
        if (rl.rlim_max < new_cur) new_cur = rl.rlim_max;
        if (new_cur > rl.rlim_cur) {
            rl.rlim_cur = new_cur;
            setrlimit(RLIMIT_NOFILE, &rl);
        }
    }
}

static void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) flags = 0;
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void set_sock_buffer(int fd) {
    int bufsize = 4096;
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));
}

static int init_server_socket(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    if (listen(fd, 1024) < 0) {
        close(fd);
        return -1;
    }
    set_nonblocking(fd);
    return fd;
}

// ============ 用户/群组操作 ============
static int find_user_unlocked(const char *username) {
    for (int i = 0; i < user_count; i++)
        if (strcmp(users[i].username, username) == 0) return i;
    return -1;
}

static int find_user(const char *username) {
    pthread_rwlock_rdlock(&user_lock);
    int idx = find_user_unlocked(username);
    pthread_rwlock_unlock(&user_lock);
    return idx;
}

static int add_user(const char *username, const char *password) {
    pthread_rwlock_wrlock(&user_lock);
    if (user_count >= MAX_USERS || find_user_unlocked(username) >= 0) {
        pthread_rwlock_unlock(&user_lock);
        return -1;
    }
    user_t *u = &users[user_count];
    strncpy(u->username, username, USERNAME_MAX - 1);
    strncpy(u->password, password, PASSWORD_MAX - 1);
    u->online = 0;
    u->fd = -1;
    u->worker_id = -1;
    u->group_count = 0;
    user_count++;
    pthread_rwlock_unlock(&user_lock);
    return user_count - 1;
}

static int find_group_unlocked(const char *name) {
    for (int i = 0; i < group_count; i++)
        if (strcmp(groups[i].name, name) == 0) return i;
    return -1;
}

static int find_group(const char *name) {
    pthread_rwlock_rdlock(&group_lock);
    int idx = find_group_unlocked(name);
    pthread_rwlock_unlock(&group_lock);
    return idx;
}

static int create_group(const char *name, int creator_idx) {
    pthread_rwlock_wrlock(&group_lock);
    if (group_count >= MAX_GROUPS || find_group_unlocked(name) >= 0) {
        pthread_rwlock_unlock(&group_lock);
        return -1;
    }
    group_t *g = &groups[group_count];
    strncpy(g->name, name, GROUPNAME_MAX - 1);
    g->member_count = 0;
    g->creator_idx = creator_idx;
    g->members[g->member_count++] = creator_idx;

    pthread_rwlock_wrlock(&user_lock);
    user_t *u = &users[creator_idx];
    if (u->group_count < MAX_GROUPS_PER_USER) u->groups[u->group_count++] = group_count;
    pthread_rwlock_unlock(&user_lock);

    group_count++;
    pthread_rwlock_unlock(&group_lock);
    return group_count - 1;
}

static int join_group(int user_idx, const char *group_name) {
    pthread_rwlock_wrlock(&group_lock);
    int gidx = find_group_unlocked(group_name);
    if (gidx < 0) {
        pthread_rwlock_unlock(&group_lock);
        return -1;
    }
    group_t *g = &groups[gidx];
    pthread_rwlock_wrlock(&user_lock);
    user_t *u = &users[user_idx];
    for (int i = 0; i < g->member_count; i++)
        if (g->members[i] == user_idx) {
            pthread_rwlock_unlock(&user_lock);
            pthread_rwlock_unlock(&group_lock);
            return -2;
        }
    if (g->member_count >= MAX_USERS || u->group_count >= MAX_GROUPS_PER_USER) {
        pthread_rwlock_unlock(&user_lock);
        pthread_rwlock_unlock(&group_lock);
        return -1;
    }
    g->members[g->member_count++] = user_idx;
    u->groups[u->group_count++] = gidx;
    pthread_rwlock_unlock(&user_lock);
    pthread_rwlock_unlock(&group_lock);
    return 0;
}

// ============ 跨线程消息传递 ============
// 使用明确的类型头
enum cross_msg_type { CROSS_MSG_NEW_FD = 0, CROSS_MSG_DATA = 1 };

static void send_cross_thread_message(int worker_id, int target_fd, const char *data, size_t data_len) {
    if (worker_id < 0 || worker_id >= THREAD_COUNT) return;
    worker_ctx_t *wctx = &workers[worker_id];

    uint32_t type = htonl(CROSS_MSG_DATA);
    uint32_t fd = htonl(target_fd);
    uint32_t len = htonl(data_len);

    struct iovec iov[4];
    iov[0].iov_base = &type;
    iov[0].iov_len = sizeof(type);
    iov[1].iov_base = &fd;
    iov[1].iov_len = sizeof(fd);
    iov[2].iov_base = &len;
    iov[2].iov_len = sizeof(len);
    iov[3].iov_base = (void *)data;
    iov[3].iov_len = data_len;

    // 忽略写入错误，重试由上层负责
    writev(wctx->notify_fd_send, iov, 4);
}

static void send_new_fd_to_worker(int worker_id, int fd) {
    if (worker_id < 0 || worker_id >= THREAD_COUNT) return;
    worker_ctx_t *wctx = &workers[worker_id];

    uint32_t type = htonl(CROSS_MSG_NEW_FD);
    uint32_t new_fd = htonl(fd);

    struct iovec iov[2];
    iov[0].iov_base = &type;
    iov[0].iov_len = sizeof(type);
    iov[1].iov_base = &new_fd;
    iov[1].iov_len = sizeof(new_fd);

    if (writev(wctx->notify_fd_send, iov, 2) < 0) {
        // 发送失败，关闭连接
        close(fd);
    }
}

// ============ 客户端发送（线程安全） ============
static void send_to_client(int fd, const char *msg, worker_ctx_t *wctx) {
    if (fd < 0 || fd >= MAX_CLIENTS) return;
    pthread_mutex_lock(&wctx->clients_lock);
    client_t *c = &wctx->clients[fd];
    if (c->fd != fd) {
        pthread_mutex_unlock(&wctx->clients_lock);
        return;
    }
    size_t len = strlen(msg);
    ssize_t sent = send(fd, msg, len, MSG_NOSIGNAL);
    if (sent < 0 && (errno != EAGAIN && errno != EWOULDBLOCK)) {
        pthread_mutex_unlock(&wctx->clients_lock);
        return;
    }
    if (sent > 0) {
        pthread_mutex_lock(&stats_lock);
        total_bytes_sent += sent;
        pthread_mutex_unlock(&stats_lock);
    }
    // 未发送完的部分放入环形缓冲区
    size_t remain = len - (sent > 0 ? sent : 0);
    const char *ptr = msg + (sent > 0 ? sent : 0);
    while (remain > 0) {
        size_t wrote = ringbuf_write(&c->write_ring, ptr, remain);
        if (wrote == 0) {
            syslog(LOG_WARNING, "write buffer full, closing fd %d", fd);
            c->state = STATE_DISCONNECTED;
            pthread_mutex_unlock(&wctx->clients_lock);
            return;
        }
        remain -= wrote;
        ptr += wrote;
    }
    if (!c->write_pending && ringbuf_readable(&c->write_ring) > 0) {
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
        ev.data.fd = fd;
        epoll_ctl(wctx->epoll_fd, EPOLL_CTL_MOD, fd, &ev);
        c->write_pending = 1;
    }
    pthread_mutex_unlock(&wctx->clients_lock);
}

// 广播到群组（支持跨线程）
static void broadcast_to_group(int sender_idx, const char *group_name, const char *content, worker_ctx_t *wctx) {
    int gidx = find_group(group_name);
    if (gidx < 0) return;
    pthread_rwlock_rdlock(&group_lock);
    group_t *g = &groups[gidx];
    pthread_rwlock_rdlock(&user_lock);
    user_t *sender = &users[sender_idx];
    char msg[MSG_MAX + 256];
    snprintf(msg, sizeof(msg), "[GROUP %s] %s: %s\n", group_name, sender->username, content);
    size_t msg_len = strlen(msg);
    for (int i = 0; i < g->member_count; i++) {
        int midx = g->members[i];
        if (midx != sender_idx && users[midx].online) {
            int target_fd = users[midx].fd;
            int target_worker = users[midx].worker_id;
            if (target_worker == wctx->thread_id) {
                send_to_client(target_fd, msg, wctx);
            } else {
                send_cross_thread_message(target_worker, target_fd, msg, msg_len);
            }
        }
    }
    pthread_rwlock_unlock(&user_lock);
    pthread_rwlock_unlock(&group_lock);
    pthread_mutex_lock(&stats_lock);
    total_messages++;
    pthread_mutex_unlock(&stats_lock);
}

// 私聊
static void send_private_msg(int sender_idx, const char *target_name, const char *content, worker_ctx_t *wctx) {
    int target_idx = find_user(target_name);
    if (target_idx < 0) {
        pthread_rwlock_rdlock(&user_lock);
        int fd = users[sender_idx].fd;
        pthread_rwlock_unlock(&user_lock);
        if (fd >= 0) send_to_client(fd, "ERROR: User not found or offline\n", wctx);
        return;
    }
    pthread_rwlock_rdlock(&user_lock);
    user_t *sender = &users[sender_idx];
    user_t *target = &users[target_idx];
    if (!target->online) {
        send_to_client(sender->fd, "ERROR: User not found or offline\n", wctx);
        pthread_rwlock_unlock(&user_lock);
        return;
    }
    char msg[MSG_MAX + 256];
    snprintf(msg, sizeof(msg), "[PM from %s]: %s\n", sender->username, content);
    size_t msg_len = strlen(msg);
    if (target->worker_id == wctx->thread_id) {
        send_to_client(target->fd, msg, wctx);
    } else {
        send_cross_thread_message(target->worker_id, target->fd, msg, msg_len);
    }
    snprintf(msg, sizeof(msg), "[PM to %s]: %s\n", target->username, content);
    send_to_client(sender->fd, msg, wctx);
    pthread_rwlock_unlock(&user_lock);
    pthread_mutex_lock(&stats_lock);
    total_messages++;
    pthread_mutex_unlock(&stats_lock);
}

// 命令处理
static void handle_command(int fd, worker_ctx_t *wctx) {
    client_t *c = &wctx->clients[fd];
    if (c->fd != fd) return;
    ssize_t nl = ringbuf_find_newline(&c->read_ring);
    if (nl < 0) return;

    char cmd_buf[BUFFER_SIZE];
    ringbuf_read(&c->read_ring, cmd_buf, nl);
    char discard;
    ringbuf_read(&c->read_ring, &discard, 1);  // consume '\n'
    cmd_buf[nl] = '\0';
    if (nl > 0 && cmd_buf[nl - 1] == '\r') cmd_buf[nl - 1] = '\0';

    char cmd[32], arg1[USERNAME_MAX], arg2[MSG_MAX];
    if (sscanf(cmd_buf, "%31s", cmd) != 1) {
        send_to_client(fd, "ERROR: Invalid command\n", wctx);
        return;
    }

    if (strcmp(cmd, "register") == 0 || strcmp(cmd, "r") == 0) {
        if (sscanf(cmd_buf, "%*s %31s %31s", arg1, arg2) == 2) {
            int idx = add_user(arg1, arg2);
            if (idx >= 0)
                send_to_client(fd, "OK: Registered successfully\n", wctx);
            else
                send_to_client(fd, "ERROR: Username exists or server full\n", wctx);
        } else
            send_to_client(fd, "ERROR: Usage: r <username> <password>\n", wctx);
    } else if (strcmp(cmd, "login") == 0 || strcmp(cmd, "l") == 0) {
        if (sscanf(cmd_buf, "%*s %31s %31s", arg1, arg2) == 2) {
            int idx = find_user(arg1);
            if (idx >= 0) {
                pthread_rwlock_wrlock(&user_lock);
                if (strcmp(users[idx].password, arg2) == 0) {
                    if (users[idx].online) {
                        send_to_client(fd, "ERROR: Already logged in elsewhere\n", wctx);
                        pthread_rwlock_unlock(&user_lock);
                        return;
                    }
                    users[idx].online = 1;
                    users[idx].fd = fd;
                    users[idx].worker_id = wctx->thread_id;
                    c->user_idx = idx;
                    c->state = STATE_LOGGED_IN;
                    send_to_client(fd, "OK: Login successful\n", wctx);
                } else
                    send_to_client(fd, "ERROR: Invalid username or password\n", wctx);
                pthread_rwlock_unlock(&user_lock);
            } else
                send_to_client(fd, "ERROR: Invalid username or password\n", wctx);
        } else
            send_to_client(fd, "ERROR: Usage: l <username> <password>\n", wctx);
    } else if (strcmp(cmd, "msg") == 0 || strcmp(cmd, "p") == 0) {
        if (c->state != STATE_LOGGED_IN)
            send_to_client(fd, "ERROR: Please login first\n", wctx);
        else if (sscanf(cmd_buf, "%*s %31s %1023[^\n]", arg1, arg2) >= 2)
            send_private_msg(c->user_idx, arg1, arg2, wctx);
        else
            send_to_client(fd, "ERROR: Usage: p <username> <message>\n", wctx);
    } else if (strcmp(cmd, "creategroup") == 0 || strcmp(cmd, "gc") == 0) {
        if (c->state != STATE_LOGGED_IN)
            send_to_client(fd, "ERROR: Please login first\n", wctx);
        else if (sscanf(cmd_buf, "%*s %31s", arg1) == 1) {
            if (create_group(arg1, c->user_idx) >= 0)
                send_to_client(fd, "OK: Group created\n", wctx);
            else
                send_to_client(fd, "ERROR: Group exists or server full\n", wctx);
        } else
            send_to_client(fd, "ERROR: Usage: gc <groupname>\n", wctx);
    } else if (strcmp(cmd, "joingroup") == 0 || strcmp(cmd, "gj") == 0) {
        if (c->state != STATE_LOGGED_IN)
            send_to_client(fd, "ERROR: Please login first\n", wctx);
        else if (sscanf(cmd_buf, "%*s %31s", arg1) == 1) {
            int ret = join_group(c->user_idx, arg1);
            if (ret == 0)
                send_to_client(fd, "OK: Joined group\n", wctx);
            else if (ret == -2)
                send_to_client(fd, "ERROR: Already in group\n", wctx);
            else
                send_to_client(fd, "ERROR: Group not found or server full\n", wctx);
        } else
            send_to_client(fd, "ERROR: Usage: gj <groupname>\n", wctx);
    } else if (strcmp(cmd, "groupmsg") == 0 || strcmp(cmd, "gm") == 0) {
        if (c->state != STATE_LOGGED_IN)
            send_to_client(fd, "ERROR: Please login first\n", wctx);
        else if (sscanf(cmd_buf, "%*s %31s %1023[^\n]", arg1, arg2) >= 2)
            broadcast_to_group(c->user_idx, arg1, arg2, wctx);
        else
            send_to_client(fd, "ERROR: Usage: gm <groupname> <message>\n", wctx);
    } else if (strcmp(cmd, "grouplist") == 0 || strcmp(cmd, "gl") == 0) {
        if (c->state != STATE_LOGGED_IN)
            send_to_client(fd, "ERROR: Please login first\n", wctx);
        else {
            char buf[4096] = "Groups you joined:\n";
            pthread_rwlock_rdlock(&user_lock);
            user_t *u = &users[c->user_idx];
            for (int i = 0; i < u->group_count; i++) {
                char line[64];
                snprintf(line, sizeof(line), "  %s\n", groups[u->groups[i]].name);
                strncat(buf, line, sizeof(buf) - strlen(buf) - 1);
            }
            pthread_rwlock_unlock(&user_lock);
            send_to_client(fd, buf, wctx);
        }
    } else if (strcmp(cmd, "listusers") == 0 || strcmp(cmd, "lu") == 0) {
        if (c->state != STATE_LOGGED_IN)
            send_to_client(fd, "ERROR: Please login first\n", wctx);
        else {
            char buf[4096] = "Online users:\n";
            pthread_rwlock_rdlock(&user_lock);
            for (int i = 0; i < user_count; i++) {
                if (users[i].online) {
                    char line[64];
                    snprintf(line, sizeof(line), "  %s\n", users[i].username);
                    strncat(buf, line, sizeof(buf) - strlen(buf) - 1);
                }
            }
            pthread_rwlock_unlock(&user_lock);
            send_to_client(fd, buf, wctx);
        }
    } else if (strcmp(cmd, "ping") == 0) {
        send_to_client(fd, "pong\n", wctx);
    } else if (strcmp(cmd, "logout") == 0) {
        if (c->state == STATE_LOGGED_IN && c->user_idx >= 0) {
            pthread_rwlock_wrlock(&user_lock);
            users[c->user_idx].online = 0;
            users[c->user_idx].fd = -1;
            users[c->user_idx].worker_id = -1;
            pthread_rwlock_unlock(&user_lock);
            c->user_idx = -1;
            c->state = STATE_CONNECTED;
            send_to_client(fd, "OK: Logged out\n", wctx);
        }
    } else if (strcmp(cmd, "quit") == 0 || strcmp(cmd, "exit") == 0) {
        send_to_client(fd, "OK: Goodbye\n", wctx);
        c->state = STATE_DISCONNECTED;
    } else {
        send_to_client(fd, "ERROR: Unknown command\n", wctx);
    }
    c->last_active = time(NULL);
}

static void handle_client_data(int fd, worker_ctx_t *wctx) {
    client_t *c = &wctx->clients[fd];
    if (c->fd != fd) return;
    char recv_buf[BUFFER_SIZE];
    while (1) {
        ssize_t n = recv(fd, recv_buf, sizeof(recv_buf), 0);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            c->state = STATE_DISCONNECTED;
            return;
        }
        if (n == 0) {
            c->state = STATE_DISCONNECTED;
            return;
        }
        size_t written = ringbuf_write(&c->read_ring, recv_buf, n);
        if (written < (size_t)n) {
            syslog(LOG_WARNING, "read buffer full, closing fd %d", fd);
            c->state = STATE_DISCONNECTED;
            return;
        }
        pthread_mutex_lock(&stats_lock);
        total_bytes_recv += n;
        pthread_mutex_unlock(&stats_lock);
        c->last_active = time(NULL);
    }
    while (ringbuf_find_newline(&c->read_ring) >= 0) handle_command(fd, wctx);
}

static void handle_write_event(int fd, worker_ctx_t *wctx) {
    client_t *c = &wctx->clients[fd];
    if (c->fd != fd) return;
    pthread_mutex_lock(&wctx->clients_lock);
    while (ringbuf_readable(&c->write_ring) > 0) {
        char out[BUFFER_SIZE];
        size_t len = ringbuf_peek(&c->write_ring, out, sizeof(out));
        ssize_t sent = send(fd, out, len, MSG_NOSIGNAL);
        if (sent < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                pthread_mutex_unlock(&wctx->clients_lock);
                return;
            }
            pthread_mutex_unlock(&wctx->clients_lock);
            c->state = STATE_DISCONNECTED;
            return;
        }
        pthread_mutex_lock(&stats_lock);
        total_bytes_sent += sent;
        pthread_mutex_unlock(&stats_lock);
        ringbuf_read(&c->write_ring, out, sent);
        if ((size_t)sent < len) {
            pthread_mutex_unlock(&wctx->clients_lock);
            return;
        }
    }
    if (ringbuf_readable(&c->write_ring) == 0 && c->write_pending) {
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = fd;
        epoll_ctl(wctx->epoll_fd, EPOLL_CTL_MOD, fd, &ev);
        c->write_pending = 0;
    }
    pthread_mutex_unlock(&wctx->clients_lock);
}

static void close_client(int fd, worker_ctx_t *wctx) {
    client_t *c = &wctx->clients[fd];
    if (c->fd != fd) return;
    if (c->user_idx >= 0) {
        pthread_rwlock_wrlock(&user_lock);
        users[c->user_idx].online = 0;
        users[c->user_idx].fd = -1;
        users[c->user_idx].worker_id = -1;
        pthread_rwlock_unlock(&user_lock);
    }
    epoll_ctl(wctx->epoll_fd, EPOLL_CTL_DEL, fd, NULL);
    close(fd);
    ringbuf_free(&c->read_ring);
    ringbuf_free(&c->write_ring);
    memset(c, 0, sizeof(client_t));
    c->fd = -1;
    atomic_fetch_sub(&wctx->client_count, 1);
}

static void check_timeouts(worker_ctx_t *wctx) {
    time_t now = time(NULL);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        client_t *c = &wctx->clients[i];
        if (c->fd >= 0 && c->state != STATE_DISCONNECTED) {
            if (now - c->last_active > TIMEOUT_SECONDS) {
                send_to_client(c->fd, "ERROR: Connection timeout\n", wctx);
                c->state = STATE_DISCONNECTED;
            }
        }
    }
}

static void *worker_thread(void *arg) {
    worker_ctx_t *wctx = (worker_ctx_t *)arg;
    struct epoll_event events[MAX_EVENTS];
    time_t last_timeout = time(NULL);
    while (running) {
        int nfds = epoll_wait(wctx->epoll_fd, events, MAX_EVENTS, 1000);
        if (nfds < 0 && errno != EINTR) break;
        for (int i = 0; i < nfds; i++) {
            int fd = events[i].data.fd;
            if (fd == wctx->notify_fd_recv) {
                // 接收主线程消息（结构：type(4) + 负载）
                uint8_t head[8];
                ssize_t n = recv(fd, head, sizeof(head), MSG_PEEK);
                if (n < 4) continue;
                uint32_t type;
                memcpy(&type, head, 4);
                type = ntohl(type);
                if (type == CROSS_MSG_NEW_FD) {
                    // 读取完整消息：type(4) + fd(4)
                    char buf[8];
                    if (recv(fd, buf, 8, 0) != 8) continue;
                    int new_fd;
                    memcpy(&new_fd, buf + 4, 4);
                    new_fd = ntohl(new_fd);
                    if (new_fd < 0 || new_fd >= MAX_CLIENTS) {
                        close(new_fd);
                        continue;
                    }
                    if (atomic_load(&wctx->client_count) >= MAX_CLIENTS_PER_WORKER) {
                        close(new_fd);
                        continue;
                    }
                    set_nonblocking(new_fd);
                    set_sock_buffer(new_fd);
                    setsockopt(new_fd, IPPROTO_TCP, TCP_NODELAY, &(int){1}, sizeof(int));
                    client_t *c = &wctx->clients[new_fd];
                    memset(c, 0, sizeof(client_t));
                    c->fd = new_fd;
                    c->state = STATE_CONNECTED;
                    c->user_idx = -1;
                    c->last_active = time(NULL);
                    c->connect_time = time(NULL);
                    ringbuf_init(&c->read_ring, BUFFER_SIZE);
                    ringbuf_init(&c->write_ring, BUFFER_SIZE);
                    struct epoll_event ev;
                    ev.events = EPOLLIN | EPOLLET;
                    ev.data.fd = new_fd;
                    epoll_ctl(wctx->epoll_fd, EPOLL_CTL_ADD, new_fd, &ev);
                    atomic_fetch_add(&wctx->client_count, 1);
                    send_to_client(new_fd,
                                   "Welcome to Chat Server\nType 'register <user> <pass>' or 'login <user> <pass>'\n",
                                   wctx);
                } else if (type == CROSS_MSG_DATA) {
                    // 读取完整消息：type(4) + target_fd(4) + len(4) + data
                    char hdr[12];
                    if (recv(fd, hdr, 12, 0) != 12) continue;
                    int target_fd;
                    uint32_t data_len;
                    memcpy(&target_fd, hdr + 4, 4);
                    memcpy(&data_len, hdr + 8, 4);
                    target_fd = ntohl(target_fd);
                    data_len = ntohl(data_len);
                    if (data_len > MSG_MAX + 256) continue;
                    char *data = malloc(data_len + 1);
                    if (!data) continue;
                    ssize_t r = recv(fd, data, data_len, 0);
                    if (r == (ssize_t)data_len) {
                        data[data_len] = '\0';
                        send_to_client(target_fd, data, wctx);
                    }
                    free(data);
                }
                continue;
            }
            if (events[i].events & EPOLLIN) handle_client_data(fd, wctx);
            if (events[i].events & EPOLLOUT) handle_write_event(fd, wctx);
            if (events[i].events & (EPOLLERR | EPOLLHUP))
                close_client(fd, wctx);
            else if (wctx->clients[fd].state == STATE_DISCONNECTED)
                close_client(fd, wctx);
        }
        time_t now = time(NULL);
        if (now - last_timeout >= 10) {
            check_timeouts(wctx);
            last_timeout = now;
        }
    }
    return NULL;
}

static void accept_new_connection(int listen_socket) {
    struct sockaddr_in addr;
    socklen_t len = sizeof(addr);
    int fd = accept(listen_socket, (struct sockaddr *)&addr, &len);
    if (fd < 0) {
        if (errno == EMFILE || errno == ENFILE) {
            syslog(LOG_WARNING, "accept failed: %s, fd limit reached", strerror(errno));
        }
        return;
    }
    if (fd >= MAX_CLIENTS) {
        close(fd);
        return;
    }
    // 选择连接数最少的工作线程，同时检查总连接数
    int best = 0;
    int best_cnt = atomic_load(&workers[0].client_count);
    int total = best_cnt;
    for (int i = 1; i < THREAD_COUNT; i++) {
        int cnt = atomic_load(&workers[i].client_count);
        total += cnt;
        if (cnt < best_cnt) {
            best_cnt = cnt;
            best = i;
        }
    }
    if (best_cnt >= MAX_CLIENTS_PER_WORKER || total >= MAX_CLIENTS) {
        close(fd);
        return;
    }
    send_new_fd_to_worker(best, fd);
}

static void init_workers() {
    for (int i = 0; i < THREAD_COUNT; i++) {
        int sv[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
            perror("socketpair");
            exit(EXIT_FAILURE);
        }
        workers[i].epoll_fd = epoll_create1(0);
        workers[i].thread_id = i;
        workers[i].notify_fd_recv = sv[1];
        workers[i].notify_fd_send = sv[0];
        workers[i].client_count = 0;
        pthread_mutex_init(&workers[i].clients_lock, NULL);
        struct epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.fd = sv[1];
        epoll_ctl(workers[i].epoll_fd, EPOLL_CTL_ADD, sv[1], &ev);
        for (int j = 0; j < MAX_CLIENTS; j++) workers[i].clients[j].fd = -1;
        pthread_create(&workers[i].thread, NULL, worker_thread, &workers[i]);
    }
}

static void handle_stats_connection(int fd) {
    char buf[4096];
    pthread_mutex_lock(&stats_lock);
    unsigned long long msgs = total_messages;
    unsigned long long br = total_bytes_recv;
    unsigned long long bs = total_bytes_sent;
    pthread_mutex_unlock(&stats_lock);
    int online = 0;
    pthread_rwlock_rdlock(&user_lock);
    for (int i = 0; i < user_count; i++)
        if (users[i].online) online++;
    pthread_rwlock_unlock(&user_lock);
    int uptime = time(NULL) - server_start_time;
    int n = snprintf(buf, sizeof(buf),
                     "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n"
                     "Chat Server Stats\n"
                     "=================\n"
                     "Uptime: %d seconds\n"
                     "Total users: %d\nOnline users: %d\n"
                     "Total groups: %d\n"
                     "Total messages: %llu\n"
                     "Bytes received: %llu\nBytes sent: %llu\n"
                     "Workers: %d\n"
                     "Worker connections: %d %d %d %d\n",
                     uptime, user_count, online, group_count, msgs, br, bs, THREAD_COUNT,
                     atomic_load(&workers[0].client_count), atomic_load(&workers[1].client_count),
                     atomic_load(&workers[2].client_count), atomic_load(&workers[3].client_count));
    send(fd, buf, n, MSG_NOSIGNAL);
    close(fd);
}

static void daemonize() {
    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        exit(EXIT_FAILURE);
    }
    if (pid > 0) exit(EXIT_SUCCESS);
    if (setsid() < 0) {
        perror("setsid");
        exit(EXIT_FAILURE);
    }
    signal(SIGHUP, SIG_IGN);
    pid = fork();
    if (pid < 0) {
        perror("fork");
        exit(EXIT_FAILURE);
    }
    if (pid > 0) exit(EXIT_SUCCESS);
    chdir("/");
    umask(0);
    int fd = open("/dev/null", O_RDWR);
    if (fd >= 0) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > STDERR_FILENO) close(fd);
    }
    openlog("chat_server", LOG_PID | LOG_NDELAY, LOG_DAEMON);
    syslog(LOG_INFO, "Chat server daemon started, PID=%d", getpid());
}

int main(int argc, char *argv[]) {
    signal(SIGPIPE, SIG_IGN);         // 忽略管道破裂 防止服务器崩溃
    signal(SIGINT, signal_handler);   // 忽略ctrl c
    signal(SIGTERM, signal_handler);  // 忽略kill（15号信号而不是9号） 9号信号仍可杀死进程
    raise_fd_limit();                 // 提升文件描述符限制，否则只能维持约1024个连接
    if (argc >= 2 && strcmp(argv[1], "-d") == 0) {
        daemonize();
    }
    listen_fd = init_server_socket(PORT);
    if (listen_fd < 0) {
        fprintf(stderr, "Failed to create listen socket on port %d\n", PORT);
        return 1;
    }
    stats_fd = init_server_socket(STATS_PORT);
    if (stats_fd < 0) {
        fprintf(stderr, "Warning: Failed to create stats socket on port %d\n", STATS_PORT);
    }
    init_workers();
    server_start_time = time(NULL);
    if (!(argc >= 2 && strcmp(argv[1], "-d") == 0)) {
        printf("Server started on port %d, stats on port %d, workers %d, fd_limit=%lu\n", PORT, STATS_PORT,
               THREAD_COUNT, (unsigned long)fd_limit);
    } else {
        syslog(LOG_INFO, "Server started on port %d, workers %d, fd_limit=%lu", PORT, THREAD_COUNT,
               (unsigned long)fd_limit);
    }
    int main_epoll = epoll_create1(0);
    struct epoll_event ev, events[2];
    ev.events = EPOLLIN;
    ev.data.fd = listen_fd;
    epoll_ctl(main_epoll, EPOLL_CTL_ADD, listen_fd, &ev);
    if (stats_fd >= 0) {
        ev.data.fd = stats_fd;
        epoll_ctl(main_epoll, EPOLL_CTL_ADD, stats_fd, &ev);
    }
    while (running) {
        int nfds = epoll_wait(main_epoll, events, 2, 1000);
        if (nfds < 0 && errno != EINTR) break;
        for (int i = 0; i < nfds; i++) {
            if (events[i].data.fd == listen_fd) {
                accept_new_connection(listen_fd);
            } else if (events[i].data.fd == stats_fd) {
                struct sockaddr_in addr;
                socklen_t len = sizeof(addr);
                int cfd = accept(stats_fd, (struct sockaddr *)&addr, &len);
                if (cfd >= 0) handle_stats_connection(cfd);
            }
        }
    }
    running = 0;
    for (int i = 0; i < THREAD_COUNT; i++) {
        pthread_join(workers[i].thread, NULL);
        close(workers[i].epoll_fd);
        close(workers[i].notify_fd_recv);
        close(workers[i].notify_fd_send);
        pthread_mutex_destroy(&workers[i].clients_lock);
    }
    close(listen_fd);
    if (stats_fd >= 0) close(stats_fd);
    close(main_epoll);
    if (!(argc >= 2 && strcmp(argv[1], "-d") == 0)) {
        printf("Server shutdown complete\n");
    } else {
        syslog(LOG_INFO, "Server shutdown complete");
        closelog();
    }
    return 0;
}