#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <termios.h>
#include <unistd.h>

#define SERVER_HOST "127.0.0.1"
#define SERVER_PORT 12345
#define BUFFER_SIZE 4096
#define USERNAME_MAX 32
#define PASSWORD_MAX 32
#define MSG_MAX 1024
#define GROUPNAME_MAX 32

typedef enum { STATE_DISCONNECTED, STATE_CONNECTED, STATE_REGISTERED, STATE_LOGGED_IN } client_state_t;

static int sockfd = -1;
static client_state_t state = STATE_DISCONNECTED;
static char username[USERNAME_MAX] = "";
static char password[PASSWORD_MAX] = "";
static volatile int running = 1;
static int auto_reconnect = 1;

static void print_prompt(void) {
    if (state == STATE_LOGGED_IN) {
        printf("\n[%s] > ", username);
    } else if (state == STATE_CONNECTED) {
        printf("\n[未登录] > ");
    } else {
        printf("\n[离线] > ");
    }
    fflush(stdout);
}

static void print_help(void) {
    printf("\n=== 聊天客户端命令 ===\n");
    printf("注册/登录:\n");
    printf("  r <用户名> <密码>  - 注册新账号\n");
    printf("  l <用户名> <密码>  - 登录\n");
    printf("\n消息命令 (需先登录):\n");
    printf("  p <用户> <消息>    - 发送私信\n");
    printf("  gc <组名>          - 创建群组\n");
    printf("  gj <组名>          - 加入群组\n");
    printf("  gm <组名> <消息>   - 群发消息\n");
    printf("  gl                 - 列出已加入的群组\n");
    printf("  ul                 - 列出在线用户\n");
    printf("\n其他:\n");
    printf("  ping               - 测试连接（手动）\n");
    printf("  logout             - 登出\n");
    printf("  quit/exit          - 退出\n");
    printf("  help               - 显示帮助\n");
    printf("=====================\n\n");
}

static int connect_to_server(void) {
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return -1;
    }
    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(SERVER_PORT);
    if (inet_pton(AF_INET, SERVER_HOST, &servaddr.sin_addr) <= 0) {
        perror("inet_pton");
        close(sockfd);
        sockfd = -1;
        return -1;
    }
    printf("Connecting to %s:%d...\n", SERVER_HOST, SERVER_PORT);
    if (connect(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) {
        perror("connect");
        close(sockfd);
        sockfd = -1;
        return -1;
    }
    int nodelay = 1;
    setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    int flags = fcntl(sockfd, F_GETFL, 0);
    fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);

    state = STATE_CONNECTED;
    printf("Connected!\n");
    return 0;
}

static void disconnect(void) {
    if (sockfd >= 0) {
        close(sockfd);
        sockfd = -1;
    }
    state = STATE_DISCONNECTED;
}

static void send_cmd(const char *fmt, ...) {
    if (sockfd < 0) return;
    char buf[BUFFER_SIZE];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);

    size_t len = strlen(buf);
    if (len == 0 || buf[len - 1] != '\n') {
        if (len < sizeof(buf) - 1) {
            strcat(buf, "\n");
        } else {
            buf[sizeof(buf) - 2] = '\n';
            buf[sizeof(buf) - 1] = '\0';
        }
    }

    ssize_t sent = send(sockfd, buf, strlen(buf), MSG_NOSIGNAL);
    if (sent < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            printf("\n[错误] 发送失败: %s\n", strerror(errno));
            disconnect();
        }
    }
}

static void handle_server_msg(const char *msg) {
    char buf[BUFFER_SIZE];
    strncpy(buf, msg, sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = '\0';
    size_t len = strlen(buf);
    if (len > 0 && buf[len - 1] == '\n') {
        buf[len - 1] = '\0';
    }
    if (len > 0 && buf[len - 2] == '\r') {
        buf[len - 2] = '\0';
    }

    printf("\r\033[K");

    if (strncmp(buf, "OK:", 3) == 0) {
        printf("[成功] %s\n", buf + 4);
        if (strstr(buf, "Login successful")) {
            state = STATE_LOGGED_IN;
        } else if (strstr(buf, "Logged out")) {
            state = STATE_CONNECTED;
            username[0] = '\0';
            password[0] = '\0';
        }
    } else if (strncmp(buf, "ERROR:", 6) == 0) {
        printf("[错误] %s\n", buf + 7);
    } else if (strncmp(buf, "[PM", 3) == 0 || strncmp(buf, "[GROUP", 6) == 0) {
        printf("\033[1;36m%s\033[0m\n", buf);
    } else if (strncmp(buf, "Welcome", 7) == 0) {
        printf("\033[1;32m%s\033[0m\n", buf);
    } else {
        printf("%s\n", buf);
    }
    print_prompt();
}

static void process_user_input(char *input) {
    size_t len = strlen(input);
    if (len > 0 && input[len - 1] == '\n') {
        input[len - 1] = '\0';
        len--;
    }
    if (len == 0) {
        print_prompt();
        return;
    }

    char cmd[32], arg1[USERNAME_MAX], arg2[MSG_MAX];
    int n = sscanf(input, "%31s", cmd);
    if (n < 1) {
        print_prompt();
        return;
    }

    if (strcmp(cmd, "help") == 0 || strcmp(cmd, "h") == 0) {
        print_help();
        print_prompt();
    } else if (strcmp(cmd, "quit") == 0 || strcmp(cmd, "exit") == 0 || strcmp(cmd, "q") == 0) {
        if (state == STATE_LOGGED_IN) {
            send_cmd("quit");
        }
        running = 0;
    } else if (strcmp(cmd, "r") == 0) {
        if (sscanf(input, "r %31s %31s", arg1, arg2) == 2) {
            strncpy(username, arg1, USERNAME_MAX - 1);
            username[USERNAME_MAX - 1] = '\0';
            strncpy(password, arg2, PASSWORD_MAX - 1);
            password[PASSWORD_MAX - 1] = '\0';
            send_cmd("register %s %s", arg1, arg2);
        } else {
            printf("用法: r <用户名> <密码>\n");
            print_prompt();
        }
    } else if (strcmp(cmd, "l") == 0) {
        if (sscanf(input, "l %31s %31s", arg1, arg2) == 2) {
            strncpy(username, arg1, USERNAME_MAX - 1);
            username[USERNAME_MAX - 1] = '\0';
            strncpy(password, arg2, PASSWORD_MAX - 1);
            password[PASSWORD_MAX - 1] = '\0';
            send_cmd("login %s %s", arg1, arg2);
        } else {
            printf("用法: l <用户名> <密码>\n");
            print_prompt();
        }
    } else if (strcmp(cmd, "p") == 0) {
        if (state != STATE_LOGGED_IN) {
            printf("请先登录\n");
            print_prompt();
            return;
        }
        char target[USERNAME_MAX], msg[MSG_MAX];
        if (sscanf(input, "p %31s %1023[^\n]", target, msg) >= 2) {
            send_cmd("msg %s %s", target, msg);
        } else {
            printf("用法: p <用户名> <消息>\n");
            print_prompt();
        }
    } else if (strcmp(cmd, "gc") == 0) {
        if (state != STATE_LOGGED_IN) {
            printf("请先登录\n");
            print_prompt();
            return;
        }
        if (sscanf(input, "gc %31s", arg1) == 1) {
            send_cmd("creategroup %s", arg1);
        } else {
            printf("用法: gc <组名>\n");
            print_prompt();
        }
    } else if (strcmp(cmd, "gj") == 0) {
        if (state != STATE_LOGGED_IN) {
            printf("请先登录\n");
            print_prompt();
            return;
        }
        if (sscanf(input, "gj %31s", arg1) == 1) {
            send_cmd("joingroup %s", arg1);
        } else {
            printf("用法: gj <组名>\n");
            print_prompt();
        }
    } else if (strcmp(cmd, "gm") == 0) {
        if (state != STATE_LOGGED_IN) {
            printf("请先登录\n");
            print_prompt();
            return;
        }
        char group[GROUPNAME_MAX], msg[MSG_MAX];
        if (sscanf(input, "gm %31s %1023[^\n]", group, msg) >= 2) {
            send_cmd("groupmsg %s %s", group, msg);
        } else {
            printf("用法: gm <组名> <消息>\n");
            print_prompt();
        }
    } else if (strcmp(cmd, "gl") == 0) {
        if (state != STATE_LOGGED_IN) {
            printf("请先登录\n");
            print_prompt();
            return;
        }
        send_cmd("grouplist");
    } else if (strcmp(cmd, "ul") == 0) {
        if (state != STATE_LOGGED_IN) {
            printf("请先登录\n");
            print_prompt();
            return;
        }
        send_cmd("listusers");
    } else if (strcmp(cmd, "ping") == 0) {
        send_cmd("ping");
    } else if (strcmp(cmd, "logout") == 0) {
        if (state == STATE_LOGGED_IN) {
            send_cmd("logout");
        }
        print_prompt();
    } else if (strcmp(cmd, "reconnect") == 0 || strcmp(cmd, "conn") == 0) {
        if (sockfd < 0) {
            connect_to_server();
        } else {
            printf("已经连接\n");
        }
        print_prompt();
    } else {
        printf("未知命令: %s，输入 help 查看帮助\n", cmd);
        print_prompt();
    }
}

static void signal_handler(int sig) {
    (void)sig;
    running = 0;
}

int main(int argc, char *argv[]) {
    (void)argc;
    (void)argv;

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGPIPE, SIG_IGN);

    printf("\n");
    printf("╔══════════════════════════════════════╗\n");
    printf("║           Chat Client v2.0           ║\n");
    printf("╚══════════════════════════════════════╝\n");
    printf("\n");
    print_help();

    if (connect_to_server() < 0) {
        printf("初始连接失败，您可以稍后使用 reconnect 命令重连\n");
    }
    print_prompt();

    char read_buf[BUFFER_SIZE];
    size_t read_pos = 0;
    time_t last_reconnect_attempt = 0;

    while (running) {
        fd_set rset;
        FD_ZERO(&rset);
        int maxfd = STDIN_FILENO;

        FD_SET(STDIN_FILENO, &rset);
        if (sockfd >= 0) {
            FD_SET(sockfd, &rset);
            if (sockfd > maxfd) maxfd = sockfd;
        }

        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;

        int ret = select(maxfd + 1, &rset, NULL, NULL, &tv);
        if (ret < 0) {
            if (errno == EINTR) continue;
            perror("select");
            break;
        }

        // 处理服务器发来的数据
        if (sockfd >= 0 && FD_ISSET(sockfd, &rset)) {
            if (read_pos >= BUFFER_SIZE - 1) {  // 这里说明出现了一个超长段且没有结尾 出现了问题 直接清空
                printf("\n[错误] 接收缓冲区溢出，清空...\n");
                read_pos = 0;
            }

            ssize_t n = recv(sockfd, read_buf + read_pos, BUFFER_SIZE - read_pos - 1, 0);
            if (n < 0) {
                if (errno != EAGAIN && errno != EWOULDBLOCK) {
                    printf("\n[错误] 接收失败: %s\n", strerror(errno));
                    disconnect();
                    print_prompt();
                }
            } else if (n == 0) {
                printf("\n[系统] 服务器断开连接\n");
                disconnect();
                print_prompt();
            } else {
                read_pos += (size_t)n;
                read_buf[read_pos] = '\0';

                char *line = read_buf;
                char *newline;
                while ((newline = strchr(line, '\n')) != NULL) {
                    *newline = '\0';
                    handle_server_msg(line);
                    line = newline + 1;
                }

                if (line > read_buf) {
                    size_t remaining = read_pos - (size_t)(line - read_buf);
                    memmove(read_buf, line, remaining);
                    read_pos = remaining;
                    read_buf[read_pos] = '\0';
                }
            }
        }

        // 处理用户输入
        if (FD_ISSET(STDIN_FILENO, &rset)) {
            char input[BUFFER_SIZE];
            if (fgets(input, sizeof(input), stdin) != NULL) {
                process_user_input(input);
            }
        }

        // 自动重连
        if (auto_reconnect && sockfd < 0) {
            time_t now = time(NULL);
            if (now - last_reconnect_attempt >= 5) {
                last_reconnect_attempt = now;
                printf("\n[系统] 尝试重新连接...\n");
                if (connect_to_server() == 0) {
                    print_prompt();
                    if (username[0] != '\0' && password[0] != '\0' && state == STATE_CONNECTED) {
                        printf("[系统] 自动重新登录...\n");
                        send_cmd("login %s %s", username, password);
                    }
                }
            }
        }
    }

    printf("\n再见!\n");
    disconnect();
    return 0;
}