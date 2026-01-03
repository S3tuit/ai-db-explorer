#define _XOPEN_SOURCE 600

#include "session_manager.h"
#include "query_result.h"
#include "safety_policy.h"
#include "postgres_backend.h"
#include "test.h"
#include "utils.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

/* Waits up to 'timeout_ms' for 'fd' to become readable, reads one chunk of
 * data and returns 1 if it contains 'needle', 0 otherwise. */
static int wait_for_output(int fd, const char *needle, int timeout_ms) {
    char buf[4096];
    size_t len = 0;
    struct pollfd pfd = { .fd = fd, .events = POLLIN };

    int rc = poll(&pfd, 1, timeout_ms);
    if (rc <= 0) return 0;
    if (!(pfd.revents & POLLIN)) return 0;

    ssize_t n = read(fd, buf, sizeof(buf) - 1);
    if (n <= 0) return 0;
    len += (size_t)n;
    buf[len] = '\0';

    return strstr(buf, needle) != NULL;
}

/* BUG tested: when the user of the app writes a terminated statements and then
 * hits Enter, out app should immidiately execute the statement. It should not
 * wait for an EOF to appear. This is a responsibility of the ByteChannel and
 * BufChannel. */
static void test_tty_input_executes_on_newline(void) {
    // master-slave PTY
    int master_fd = posix_openpt(O_RDWR | O_NOCTTY);
    ASSERT_TRUE(master_fd >= 0);
    ASSERT_TRUE(grantpt(master_fd) == 0);
    ASSERT_TRUE(unlockpt(master_fd) == 0);

    char *slave_name = ptsname(master_fd);
    ASSERT_TRUE(slave_name != NULL);

    int slave_fd = open(slave_name, O_RDWR | O_NOCTTY);
    ASSERT_TRUE(slave_fd >= 0);

    int out_pipe[2];
    ASSERT_TRUE(pipe(out_pipe) == 0);

    pid_t pid = fork();
    ASSERT_TRUE(pid >= 0);

    if (pid == 0) {
        // child is slave, simulates our app; creates a Session and waits for
        // input from terminal (from the master in this case)
        close(master_fd);
        close(out_pipe[0]);

        FILE *in = fdopen(slave_fd, "r");
        FILE *out = fdopen(out_pipe[1], "w");
        ASSERT_TRUE(in != NULL);
        ASSERT_TRUE(out != NULL);

        DbBackend *pg = postgres_backend_create();
        ASSERT_TRUE(pg != NULL);
    
        SafetyPolicy policy = {0};
        safety_policy_init(&policy, NULL, NULL, NULL, NULL);
        int rc = pg->vt->connect(pg, "dbname=postgres", &policy);
        ASSERT_TRUE(rc == OK);

        SessionManager sm;
        ASSERT_TRUE(session_init(&sm, in, out, pg) == OK);
        ASSERT_TRUE(session_run(&sm) == OK);

        session_clean(&sm);
        fclose(in);
        fclose(out);
        pg->vt->destroy(pg);
        _exit(0);
    }

    // parent is master, simulates the user typing in the terminal to interact
    // with the app
    close(slave_fd);
    close(out_pipe[1]);

    // the user writes, hit Enter, our app should consume even if no EOF are
    // seen up until that point
    const char *sql = "SELECT 1;\n";
    ASSERT_TRUE(write(master_fd, sql, strlen(sql)) == (ssize_t)strlen(sql));

    ASSERT_TRUE(wait_for_output(out_pipe[0], "Content-Length:", 1000));

    close(master_fd);
    close(out_pipe[0]);

    int status = 0;
    ASSERT_TRUE(waitpid(pid, &status, 0) == pid);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_TRUE(WEXITSTATUS(status) == 0);
}

int main(void) {
    test_tty_input_executes_on_newline();
    fprintf(stderr, "OK: test_tty_input\n");
    return 0;
}
