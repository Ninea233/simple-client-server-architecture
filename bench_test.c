/*
 * chat_bench — 聊天服务器高并发压测工具
 *
 * 测试模式：
 *   conn    连接风暴          测量建立 TCP 连接的速率
 *   login   注册+登录风暴     测量用户注册和登录的吞吐量
 *   ping   RTT 延迟测试      测量 ping/pong 往返延迟分布
 *   msg     消息互发测试      测量私聊消息吞吐量和跨线程转发延迟
 *   stress  混合压力测试      连接→登录→发消息→退出→重连循环
 *
 * 用法：
 *   ./chat_bench -s 127.0.0.1 -p 12345 -t stress -n 5000 -c 200 -d 60
 *
 * 编译：
 *   gcc -O2 -o chat_bench bench_test.c -lpthread
 *
 * 依赖：Linux (epoll, clock_gettime)
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

/* ======================== 配置 ======================== */

#define MAX_CONNECTIONS 20000
#define BUFFER_SIZE 4096
#define USERNAME_MAX 32
#define PASSWORD_MAX 32
#define MAX_SAMPLES 2000000  // 最多采集的延迟样本数

typedef enum {
    TEST_CONN,    // 连接风暴
    TEST_LOGIN,   // 登录风暴
    TEST_PING,    // 延迟测试
    TEST_MSG,     // 消息互发
    TEST_STRESS,  // 混合压力
} test_type_t;

static const char *test_names[] = {"conn", "login", "ping", "msg", "stress"};

/* ======================== 连接上下文 ======================== */

typedef enum {
    ST_INIT,        // 初始态
    ST_CONNECTING,  // connect() 进行中
    ST_REG_SENT,    // 已发送 register 命令
    ST_LOGIN_SENT,  // 已发送 login 命令
    ST_LOGGED_IN,   // 已登录
    ST_PING_SENT,   // 已发送 ping，等待 pong
    ST_MSG_SENT,    // 已发送消息
    ST_QUIT_SENT,   // 已发送 quit
    ST_CLOSED,      // 连接已关闭
} conn_state_t;

typedef struct {
    int fd;
    conn_state_t state;
    int idx;  // 连接编号
    char username[USERNAME_MAX];
    char password[PASSWORD_MAX];

    char recv_buf[BUFFER_SIZE];
    size_t recv_len;

    struct timespec t0;  // 当前操作开始时间（用于测量延迟）
    double rtt;          // 最近一次 ping RTT

    int msg_pending;       // msg 测试: 是否有消息在等待确认
    int msg_target;        // msg 测试: 目标用户 idx
    double msg_send_time;  // msg 测试: 发送时间戳

    int ops_done;  // stress 模式下的操作计数
} conn_t;

/* ======================== 统计 ======================== */

typedef struct {
    long long conn_attempts;
    long long conn_ok;
    long long conn_fail;
    long long reg_ok;
    long long reg_fail;
    long long login_ok;
    long long login_fail;
    long long ping_sent;
    long long pong_received;
    long long msg_sent;       // msg 测试: PM 发送次数
    long long msg_confirmed;  // msg 测试: 确认收到 [PM to ...]
    long long msg_delivered;  // msg 测试: 收到别人发来的 [PM from ...]
    long long errors;

    int sample_count;
    double lat_samples[MAX_SAMPLES];  // 延迟样本 (ms)
} stats_t;

/* ======================== 全局状态 ======================== */

static volatile int running = 1;

static void sig_handler(int sig) {
    (void)sig;
    running = 0;
}
static int epoll_fd = -1;
static conn_t connections[MAX_CONNECTIONS];
static int conn_count = 0;
static stats_t stats = {0};

static test_type_t test_type = TEST_STRESS;
static const char *server_host = "127.0.0.1";
static int server_port = 12345;
static int total_target = 1000;
static int batch_size = 100;
static int test_duration = 30;

static double test_start_time;

/* ======================== 工具函数 ======================== */

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

static double timespec_diff_ms(const struct timespec *end, const struct timespec *start) {
    return (end->tv_sec - start->tv_sec) * 1000.0 + (end->tv_nsec - start->tv_nsec) / 1e6;
}

static void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void set_tcp_nodelay(int fd) {
    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
}

static int tcp_connect(void) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    set_nonblocking(fd);
    set_tcp_nodelay(fd);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(server_port);
    if (inet_pton(AF_INET, server_host, &addr.sin_addr) <= 0) {
        close(fd);
        return -1;
    }
    connect(fd, (struct sockaddr *)&addr, sizeof(addr));
    return fd;
}

static int conn_send(conn_t *c, const char *fmt, ...) {
    char buf[BUFFER_SIZE];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    size_t len = strlen(buf);
    if (len == 0 || buf[len - 1] != '\n') {
        if (len < sizeof(buf) - 1) {
            strcat(buf, "\n");
            len++;
        }
    }
    ssize_t n = send(c->fd, buf, len, MSG_NOSIGNAL);
    if (n <= 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
        return -1;
    }
    return (int)n;
}

/* ======================== epoll 管理 ======================== */

static int epoll_add(int fd, uint32_t events) {
    struct epoll_event ev;
    ev.events = events;
    ev.data.fd = fd;
    return epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev);
}

/* ======================== 延迟样本记录 ======================== */

static void record_latency(double ms) {
    if (stats.sample_count < MAX_SAMPLES) {
        stats.lat_samples[stats.sample_count++] = ms;
    }
}

/* ======================== 连接管理 ======================== */

static conn_t *conn_alloc(int idx) {
    if (idx < 0 || idx >= MAX_CONNECTIONS) return NULL;
    conn_t *c = &connections[idx];
    memset(c, 0, sizeof(*c));
    c->fd = -1;
    c->idx = idx;
    c->state = ST_INIT;
    return c;
}

static int conn_create(conn_t *c) {
    int fd = tcp_connect();
    if (fd < 0) return -1;
    c->fd = fd;
    c->state = ST_CONNECTING;
    epoll_add(fd, EPOLLIN | EPOLLOUT | EPOLLET);
    clock_gettime(CLOCK_MONOTONIC, &c->t0);
    return 0;
}

static void conn_close(conn_t *c) {
    if (c->fd >= 0) {
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, c->fd, NULL);
        close(c->fd);
        c->fd = -1;
    }
    c->state = ST_CLOSED;
}

/* ======================== 连接事件处理 ======================== */

static void handle_connect_event(conn_t *c) {
    int err;
    socklen_t errlen = sizeof(err);
    if (getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &err, &errlen) < 0 || err != 0) {
        stats.conn_fail++;
        stats.errors++;
        conn_close(c);
        return;
    }
    stats.conn_ok++;
    c->state = ST_INIT;
}

static void handle_read(conn_t *c) {
    char buf[BUFFER_SIZE];
    ssize_t n = recv(c->fd, buf, sizeof(buf) - 1, 0);
    if (n <= 0) {
        if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
            stats.errors++;
            conn_close(c);
        }
        return;
    }
    buf[n] = '\0';

    size_t remain = sizeof(c->recv_buf) - c->recv_len - 1;
    if (n > (ssize_t)remain) n = remain;
    memcpy(c->recv_buf + c->recv_len, buf, n);
    c->recv_len += n;
    c->recv_buf[c->recv_len] = '\0';

    char *line = c->recv_buf;
    char *nl;
    while ((nl = strchr(line, '\n')) != NULL) {
        *nl = '\0';
        size_t llen = strlen(line);
        if (llen > 0 && line[llen - 1] == '\r') line[llen - 1] = '\0';

        if (c->state == ST_CONNECTING) {
            // 收到 Welcome 消息，连接已建立
            stats.conn_ok++;
            c->state = ST_INIT;
        } else if (c->state == ST_REG_SENT) {
            if (strncmp(line, "OK:", 3) == 0) {
                stats.reg_ok++;
                conn_send(c, "login %s %s", c->username, c->password);
                clock_gettime(CLOCK_MONOTONIC, &c->t0);
                c->state = ST_LOGIN_SENT;
            } else if (strncmp(line, "ERROR:", 6) == 0) {
                stats.reg_fail++;
                // 用户可能已存在（前次运行遗留），尝试直接登录
                conn_send(c, "login %s %s", c->username, c->password);
                clock_gettime(CLOCK_MONOTONIC, &c->t0);
                c->state = ST_LOGIN_SENT;
            }
            /* 忽略非响应行（如 Welcome 横幅），继续等待 */
        } else if (c->state == ST_LOGIN_SENT) {
            if (strncmp(line, "OK:", 3) == 0) {
                stats.login_ok++;
                c->state = ST_LOGGED_IN;
            } else if (strncmp(line, "ERROR:", 6) == 0) {
                stats.login_fail++;
                stats.errors++;
                conn_close(c);
                return;
            }
            /* 忽略非响应行 */
        } else if (c->state == ST_PING_SENT) {
            if (strcmp(line, "pong") == 0) {
                struct timespec now;
                clock_gettime(CLOCK_MONOTONIC, &now);
                double rtt = timespec_diff_ms(&now, &c->t0);
                c->rtt = rtt;
                stats.pong_received++;
                record_latency(rtt);
                c->state = ST_LOGGED_IN;
            }
        } else if (c->state == ST_MSG_SENT) {
            if (strncmp(line, "ERROR:", 6) == 0) {
                stats.errors++;
                c->state = ST_LOGGED_IN;
                if (test_type == TEST_STRESS) c->ops_done = 0;
            }
        } else if (c->state == ST_QUIT_SENT) {
            conn_close(c);
            return;
        }

        /* ----- 通用响应处理（任何状态下都可能收到） ----- */
        /* [PM to ...] = 我发出的私聊已送达确认 */
        if (strncmp(line, "[PM to", 6) == 0) {
            stats.msg_confirmed++;
            if (test_type == TEST_MSG) {
                struct timespec now;
                clock_gettime(CLOCK_MONOTONIC, &now);
                record_latency(timespec_diff_ms(&now, &c->t0));
                c->msg_pending = 0;
                if (c->state == ST_LOGGED_IN || c->state == ST_MSG_SENT) {
                    c->state = ST_LOGGED_IN;
                }
            } else {
                if (c->state == ST_MSG_SENT) {
                    c->state = ST_LOGGED_IN;
                }
            }
        }
        /* [PM from ...] = 别人发给我的私聊 */
        if (strncmp(line, "[PM from", 8) == 0) {
            stats.msg_delivered++;
        }

        line = nl + 1;
    }

    if (line > c->recv_buf) {
        size_t remaining = c->recv_len - (line - c->recv_buf);
        memmove(c->recv_buf, line, remaining);
        c->recv_len = remaining;
    }
}

/* ======================== 测试阶段函数 ======================== */

/* -------- 连接风暴 -------- */
static void conn_test_spawn(void) {
    for (int i = 0; i < total_target && stats.conn_attempts < total_target; i++) {
        conn_t *c = &connections[i];
        if (c->fd < 0 || c->state == ST_CLOSED) {
            if (conn_create(c) == 0) {
                conn_count++;
                stats.conn_attempts++;
            }
        }
    }
}

static int conn_test_tick(void) {
    // 1. 关闭所有完成的连接 (ST_INIT → CLOSED)
    for (int i = 0; i < total_target; i++) {
        conn_t *c = &connections[i];
        if (c->fd >= 0 && c->state == ST_INIT) {
            conn_close(c);
        }
    }
    // 2. 统计已关闭
    int done = 0;
    for (int i = 0; i < total_target && done < total_target; i++) {
        if (connections[i].state == ST_CLOSED) done++;
    }
    // 3. 补充新连接
    conn_test_spawn();
    return (done >= total_target) ? 1 : 0;
}

/* -------- 登录风暴 -------- */
static void login_test_spawn(void) {
    for (int i = 0; i < total_target; i++) {
        conn_t *c = &connections[i];
        if (c->fd < 0 || c->state == ST_CLOSED) {
            if (conn_create(c) == 0) {
                conn_count++;
                stats.conn_attempts++;
            }
        } else if (c->state == ST_INIT) {
            snprintf(c->username, sizeof(c->username), "bench_u%d", i);
            snprintf(c->password, sizeof(c->password), "pass%d", i);
            conn_send(c, "register %s %s", c->username, c->password);
            clock_gettime(CLOCK_MONOTONIC, &c->t0);
            c->state = ST_REG_SENT;
        }
    }
}

static int login_test_tick(void) {
    login_test_spawn();
    int done = 0;
    for (int i = 0; i < total_target; i++) {
        if (connections[i].state == ST_LOGGED_IN) done++;
    }
    return (done >= total_target) ? 1 : 0;
}

/* -------- Ping 延迟测试 -------- */
static int ping_test_tick(void) {
    static int last_ping_idx = 0;
    int sent = 0;
    for (int i = 0; i < total_target && sent < batch_size; i++) {
        int idx = (last_ping_idx + i) % total_target;
        conn_t *c = &connections[idx];
        if (c->state == ST_LOGGED_IN) {
            clock_gettime(CLOCK_MONOTONIC, &c->t0);
            conn_send(c, "ping");
            c->state = ST_PING_SENT;
            stats.ping_sent++;
            sent++;
        }
    }
    last_ping_idx = (last_ping_idx + sent) % total_target;

    for (int i = 0; i < total_target; i++) {
        if (connections[i].state == ST_PING_SENT) return 0;
    }
    return 1;
}

/* -------- 消息互发测试 -------- */
static void msg_init_login(void) {
    // 发送 login 给所有已连接未登录的用户
    for (int i = 0; i < total_target; i++) {
        conn_t *c = &connections[i];
        if (c->state == ST_INIT) {
            snprintf(c->username, sizeof(c->username), "muser_%d", i);
            snprintf(c->password, sizeof(c->password), "mpass_%d", i);
            conn_send(c, "register %s %s", c->username, c->password);
            clock_gettime(CLOCK_MONOTONIC, &c->t0);
            c->state = ST_REG_SENT;
        }
    }
}

static void msg_test_tick(void) {
    // 超时检测：关闭长时间无响应的连接（服务器 fd 不够时，连接会卡在 backlog 里）
    struct timespec now_ts;
    clock_gettime(CLOCK_MONOTONIC, &now_ts);
    for (int i = 0; i < total_target; i++) {
        conn_t *c = &connections[i];
        if (c->fd >= 0 && (c->state == ST_CONNECTING || c->state == ST_REG_SENT || c->state == ST_LOGIN_SENT)) {
            if (timespec_diff_ms(&now_ts, &c->t0) > 5000.0) {
                stats.errors++;
                conn_close(c);
            }
        }
    }

    // 先确保所有用户已登录
    // 注意：必须先检查 fd<0 / ST_CLOSED（未连接），再检查状态（已连接但未登录），
    // 否则 conn_alloc 分配的 fd=-1 会被 state==ST_INIT 提前匹配而无法创建连接
    int all_logged_in = 1;
    for (int i = 0; i < total_target; i++) {
        if (connections[i].fd < 0 || connections[i].state == ST_CLOSED) {
            // 连接断开或从未连接 → 重建
            conn_t *c = &connections[i];
            if (conn_create(c) == 0) {
                conn_count++;
                stats.conn_attempts++;
            }
            all_logged_in = 0;
        } else if (connections[i].state == ST_INIT || connections[i].state == ST_CONNECTING ||
                   connections[i].state == ST_REG_SENT || connections[i].state == ST_LOGIN_SENT) {
            all_logged_in = 0;
        }
    }

    if (!all_logged_in) {
        msg_init_login();
        return;
    }

    // 所有用户已登录 → 互发消息（环形拓扑）
    // 每个用户向 (i+1) % total_target 发消息
    for (int i = 0; i < total_target; i++) {
        conn_t *c = &connections[i];
        if (c->state == ST_LOGGED_IN && !c->msg_pending) {
            int target = (i + 1) % total_target;
            c->msg_pending = 1;
            c->msg_target = target;
            clock_gettime(CLOCK_MONOTONIC, &c->t0);
            conn_send(c, "p %s msg_%lld_%d", connections[target].username, stats.msg_sent, i);
            stats.msg_sent++;
        }
    }
}

/* -------- 混合压力测试 -------- */
static void stress_try_connect(conn_t *c) {
    c->ops_done = 0;  // 新连接周期开始
    if (conn_create(c) == 0) {
        conn_count++;
        stats.conn_attempts++;
    }
}

static void stress_try_login(conn_t *c) {
    snprintf(c->username, sizeof(c->username), "su_%d_%d", c->idx, rand() % 100000);
    snprintf(c->password, sizeof(c->password), "sp%d", rand() % 100000);
    conn_send(c, "register %s %s", c->username, c->password);
    clock_gettime(CLOCK_MONOTONIC, &c->t0);
    c->state = ST_REG_SENT;
}

static void stress_try_send_msg(conn_t *c) {
    // 向环中的下一个用户发送私聊
    int target_idx = (c->idx + 1) % total_target;
    const char *target_name = connections[target_idx].username;
    if (target_name[0] == '\0') target_name = c->username;  // 自救兜底
    conn_send(c, "p %s stress_msg_%lld_%d", target_name, stats.msg_sent, c->idx);
    c->state = ST_MSG_SENT;
    c->ops_done = 1;
    stats.msg_sent++;
    clock_gettime(CLOCK_MONOTONIC, &c->t0);
}

static void stress_try_quit(conn_t *c) {
    conn_send(c, "quit");
    c->state = ST_QUIT_SENT;
}

static void stress_tick(void) {
    // 阶段0: 超时恢复 — 消息确认卡住5秒后重置
    for (int i = 0; i < total_target; i++) {
        conn_t *c = &connections[i];
        if (c->state == ST_MSG_SENT && c->fd >= 0) {
            struct timespec now;
            clock_gettime(CLOCK_MONOTONIC, &now);
            if (timespec_diff_ms(&now, &c->t0) > 5000.0) {
                stats.errors++;
                c->ops_done = 0;
                c->state = ST_LOGGED_IN;
            }
        }
    }

    // 阶段1: 补充连接（始终重连已关闭的连接，实现循环压力）
    for (int i = 0; i < total_target; i++) {
        conn_t *c = &connections[i];
        if (c->fd < 0 || c->state == ST_CLOSED) {
            stress_try_connect(c);
        }
    }

    // 阶段2: 对已连接未登录的连接发送登录
    for (int i = 0; i < total_target; i++) {
        conn_t *c = &connections[i];
        if (c->state == ST_INIT && c->fd >= 0) {
            stress_try_login(c);
        }
    }

    // 阶段3: 对已登录的发送消息或退出
    static int stress_idx = 0;
    int checked = 0;
    for (int i = 0; i < total_target && checked < batch_size; i++) {
        int idx = (stress_idx + i) % total_target;
        conn_t *c = &connections[idx];
        if (c->state == ST_LOGGED_IN) {
            if (c->ops_done) {
                stress_try_quit(c);
            } else {
                stress_try_send_msg(c);
            }
            checked++;
        }
    }
    stress_idx = (stress_idx + checked) % total_target;
}

/* ======================== 统计报表 ======================== */

static int cmp_double(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    return (da > db) - (da < db);
}

static void print_stats(double elapsed) {
    printf("\n========================================\n");
    printf("  压测报告\n");
    printf("========================================\n");
    printf("  测试类型:      %s\n", test_names[test_type]);
    printf("  目标数:        %d\n", total_target);
    printf("  持续时间:      %.1f 秒\n", elapsed);
    printf("----------------------------------------\n");
    printf("  连接尝试:      %lld\n", stats.conn_attempts);
    printf("  连接成功:      %lld\n", stats.conn_ok);
    printf("  连接失败:      %lld\n", stats.conn_fail);
    printf("  注册成功:      %lld\n", stats.reg_ok);
    printf("  注册失败:      %lld\n", stats.reg_fail);
    printf("  登录成功:      %lld\n", stats.login_ok);
    printf("  登录失败:      %lld\n", stats.login_fail);
    printf("  Ping 发送:     %lld\n", stats.ping_sent);
    printf("  Pong 接收:     %lld\n", stats.pong_received);
    printf("  消息发送:      %lld\n", stats.msg_sent);
    printf("  消息确认:      %lld\n", stats.msg_confirmed);
    printf("  消息送达:      %lld\n", stats.msg_delivered);
    printf("  总错误数:      %lld\n", stats.errors);
    printf("----------------------------------------\n");

    if (elapsed > 0) {
        if (stats.conn_ok > 0) printf("  连接速率:      %.1f conn/s\n", stats.conn_ok / elapsed);
        if (stats.login_ok > 0) printf("  登录速率:      %.1f login/s\n", stats.login_ok / elapsed);
        if (stats.msg_confirmed > 0) printf("  消息吞吐量:    %.1f msg/s\n", stats.msg_confirmed / elapsed);
        if (stats.pong_received > 0) printf("  Ping 速率:     %.1f ping/s\n", stats.pong_received / elapsed);
    }

    if (stats.sample_count > 0) {
        qsort(stats.lat_samples, stats.sample_count, sizeof(double), cmp_double);
        double min = stats.lat_samples[0];
        double max = stats.lat_samples[stats.sample_count - 1];
        double sum = 0;
        for (int i = 0; i < stats.sample_count; i++) sum += stats.lat_samples[i];
        double avg = sum / stats.sample_count;
        double p50 = stats.lat_samples[stats.sample_count / 2];
        double p90 = stats.lat_samples[stats.sample_count * 9 / 10];
        double p95 = stats.lat_samples[stats.sample_count * 95 / 100];
        double p99 = stats.lat_samples[stats.sample_count * 99 / 100];
        double p999 = stats.lat_samples[stats.sample_count * 999 / 1000];

        printf("----------------------------------------\n");
        printf("  延迟分布 (ms):\n");
        printf("    样本数:      %d\n", stats.sample_count);
        printf("    Min:         %.3f\n", min);
        printf("    Avg:         %.3f\n", avg);
        printf("    Max:         %.3f\n", max);
        printf("    P50:         %.3f\n", p50);
        printf("    P90:         %.3f\n", p90);
        printf("    P95:         %.3f\n", p95);
        printf("    P99:         %.3f\n", p99);
        printf("    P99.9:       %.3f\n", p999);
    }
    printf("========================================\n");
}

/* ======================== 测试运行器 ======================== */

static void run_test(void) {
    test_start_time = now_sec();

    // 预分配连接上下文
    int target = total_target < MAX_CONNECTIONS ? total_target : MAX_CONNECTIONS;
    for (int i = 0; i < target; i++) {
        conn_alloc(i);
    }

    // 初始建立一批连接
    int init_batch = batch_size < target ? batch_size : target;
    for (int i = 0; i < init_batch; i++) {
        conn_t *c = &connections[i];
        if (conn_create(c) == 0) {
            conn_count++;
            stats.conn_attempts++;
        }
    }

    struct epoll_event events[1024];
    double last_print = now_sec();
    double stress_last_tick = now_sec();

    while (running) {
        double elapsed = now_sec() - test_start_time;
        if (elapsed > test_duration) break;

        int nfds = epoll_wait(epoll_fd, events, 1024, 100);
        if (nfds < 0 && errno != EINTR) break;

        for (int i = 0; i < nfds; i++) {
            int fd = events[i].data.fd;
            conn_t *c = NULL;
            int search_limit = total_target < MAX_CONNECTIONS ? total_target : MAX_CONNECTIONS;
            for (int j = 0; j < search_limit; j++) {
                if (connections[j].fd == fd) {
                    c = &connections[j];
                    break;
                }
            }
            if (!c) continue;

            if (events[i].events & EPOLLOUT && c->state == ST_CONNECTING) {
                handle_connect_event(c);
                if (c->state == ST_CLOSED) continue;
            }
            if (events[i].events & EPOLLIN) {
                handle_read(c);
            }
            if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                stats.errors++;
                conn_close(c);
            }
        }

        // 根据测试类型执行 tick
        switch (test_type) {
            case TEST_CONN:
                if (conn_test_tick()) goto test_done;
                break;
            case TEST_LOGIN:
                if (login_test_tick()) goto test_done;
                break;
            case TEST_PING:
                // ping 测试 —— 在主循环中边连接/登录边测
                {
                    static int ping_round = 1;
                    if (ping_round) {
                        int logged_in = 0;
                        for (int i = 0; i < total_target; i++) {
                            if (connections[i].state == ST_LOGGED_IN) logged_in++;
                        }
                        if (logged_in < total_target) {
                            // 确保已连接
                            for (int i = 0; i < total_target; i++) {
                                conn_t *c = &connections[i];
                                if (c->fd < 0 || c->state == ST_CLOSED) {
                                    if (conn_create(c) == 0) {
                                        conn_count++;
                                        stats.conn_attempts++;
                                    }
                                } else if (c->state == ST_INIT) {
                                    snprintf(c->username, sizeof(c->username), "pu_%d", i);
                                    snprintf(c->password, sizeof(c->password), "pp%d", i);
                                    conn_send(c, "register %s %s", c->username, c->password);
                                    clock_gettime(CLOCK_MONOTONIC, &c->t0);
                                    c->state = ST_REG_SENT;
                                }
                            }
                        } else {
                            ping_round = ping_test_tick();
                        }
                    } else {
                        ping_round = ping_test_tick();
                    }
                }
                break;
            case TEST_MSG:
                msg_test_tick();
                break;
            case TEST_STRESS:
                if (now_sec() - stress_last_tick > 0.1) {
                    stress_tick();
                    stress_last_tick = now_sec();
                }
                break;
        }

        // 进度输出
        if (now_sec() - last_print >= 2.0) {
            double el = now_sec() - test_start_time;
            printf("\r  [%.0f/%.0fs] conn=%lld login=%lld msg=%lld cfm=%lld del=%lld err=%lld  ", el,
                   (double)test_duration, stats.conn_ok, stats.login_ok, stats.msg_sent, stats.msg_confirmed,
                   stats.msg_delivered, stats.errors);
            fflush(stdout);
            last_print = now_sec();
        }
    }

test_done:
    // 清理（只清理分配过的连接，避免 close(0)=stdin）
    int clean_target = total_target < MAX_CONNECTIONS ? total_target : MAX_CONNECTIONS;
    for (int i = 0; i < clean_target; i++) {
        conn_close(&connections[i]);
    }

    double total_elapsed = now_sec() - test_start_time;
    printf("\n");
    print_stats(total_elapsed);
}

/* ======================== 文件描述符限制提升 ======================== */

static void raise_fd_limit(void) {
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        if (rl.rlim_cur < 65536) {
            rlim_t new_cur = 65536;
            if (rl.rlim_max < new_cur) new_cur = rl.rlim_max;
            if (new_cur > rl.rlim_cur) {
                rl.rlim_cur = new_cur;
                setrlimit(RLIMIT_NOFILE, &rl);
            }
        }
    }
}

/* ======================== 主函数 ======================== */

static void print_usage(const char *prog) {
    printf("用法: %s [选项]\n", prog);
    printf("选项:\n");
    printf("  -s, --server <host>    服务器地址 (默认: 127.0.0.1)\n");
    printf("  -p, --port <port>      服务器端口 (默认: 12345)\n");
    printf("  -t, --test <type>      测试类型: conn|login|ping|msg|stress\n");
    printf("  -n, --num <count>      用户总数 (conn/login/msg/stress 的模拟用户数)\n");
    printf("  -c, --conc <count>     每轮并发数 (每轮批量创建/操作的数量) (默认: 100)\n");
    printf("  -d, --duration <sec>   测试持续秒数 (默认: 30s)\n");
    printf("  -h, --help             显示此帮助\n");
    printf("\n示例:\n");
    printf("  %s -t conn -n 10000 -c 500           # 连接风暴\n", prog);
    printf("  %s -t login -n 5000 -c 200           # 登录风暴\n", prog);
    printf("  %s -t ping -n 2000 -c 500 -d 60      # 延迟测试\n", prog);
    printf("  %s -t msg -n 1000 -c 500 -d 60       # 消息互发(跨线程)\n", prog);
    printf("  %s -t stress -n 3000 -c 300 -d 120   # 混合压力\n", prog);
}

int main(int argc, char *argv[]) {
    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);
    signal(SIGPIPE, SIG_IGN);

    // 尝试提升文件描述符限制（默认 ulimit -n 通常只有 1024，远不够用）
    raise_fd_limit();

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-s") == 0 || strcmp(argv[i], "--server") == 0) {
            if (++i >= argc) {
                fprintf(stderr, "缺少服务器地址\n");
                return 1;
            }
            server_host = argv[i];
        } else if (strcmp(argv[i], "-p") == 0 || strcmp(argv[i], "--port") == 0) {
            if (++i >= argc) {
                fprintf(stderr, "缺少端口号\n");
                return 1;
            }
            server_port = atoi(argv[i]);
        } else if (strcmp(argv[i], "-t") == 0 || strcmp(argv[i], "--test") == 0) {
            if (++i >= argc) {
                fprintf(stderr, "缺少测试类型\n");
                return 1;
            }
            for (size_t j = 0; j < sizeof(test_names) / sizeof(test_names[0]); j++) {
                if (strcmp(argv[i], test_names[j]) == 0) {
                    test_type = (test_type_t)j;
                    break;
                }
            }
        } else if (strcmp(argv[i], "-n") == 0 || strcmp(argv[i], "--num") == 0) {
            if (++i >= argc) {
                fprintf(stderr, "缺少数量\n");
                return 1;
            }
            total_target = atoi(argv[i]);
        } else if (strcmp(argv[i], "-c") == 0 || strcmp(argv[i], "--conc") == 0) {
            if (++i >= argc) {
                fprintf(stderr, "缺少并发数\n");
                return 1;
            }
            batch_size = atoi(argv[i]);
        } else if (strcmp(argv[i], "-d") == 0 || strcmp(argv[i], "--duration") == 0) {
            if (++i >= argc) {
                fprintf(stderr, "缺少持续时间\n");
                return 1;
            }
            test_duration = atoi(argv[i]);
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "未知选项: %s\n", argv[i]);
            print_usage(argv[0]);
            return 1;
        }
    }

    if (total_target > MAX_CONNECTIONS) {
        fprintf(stderr, "用户总数 %d 超过最大限制 %d\n", total_target, MAX_CONNECTIONS);
        return 1;
    }

    // 检查并警告 fd 限制可能不足
    {
        struct rlimit rl;
        if (getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur < (rlim_t)total_target + 20) {
            fprintf(stderr, "\n警告: 系统文件描述符限制过小 (cur=%lu, max=%lu)，建议先运行: ulimit -n 65536\n\n",
                    (unsigned long)rl.rlim_cur, (unsigned long)rl.rlim_max);
        }
    }

    epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        perror("epoll_create1");
        return 1;
    }

    printf("========================================\n");
    printf("  Chat Server 压测工具\n");
    printf("========================================\n");
    printf("  服务器:    %s:%d\n", server_host, server_port);
    printf("  测试类型:  %s\n", test_names[test_type]);
    printf("  用户总数:  %d\n", total_target);
    printf("  每轮并发:  %d\n", batch_size);
    printf("  持续时间:  %d 秒\n", test_duration);
    printf("========================================\n\n");

    run_test();

    close(epoll_fd);
    return 0;
}
