/*
 * Copyright (C) 2014 Mail.RU
 * Copyright (C) 2014 Yuriy Vostrikov
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/types.h>

#import <net_io.h>
#import <spawn_child.h>
#import <fiber.h>
#import <say.h>
#import <util.h>
#import <palloc.h>
#import <net_io.h>
#import <octopus.h>

struct fork_request {
	char name[64];
	int (*handler)(int fd, void *state, int len);
};

struct fork_reply {
	pid_t pid;
};

static pid_t
fork_pair(int type, int *sock)
{
	int socks[2];
	int one = 1;
	pid_t pid;

	if (socketpair(AF_UNIX, type, 0, socks) == -1) {
		say_syserror("socketpair");
		return -1;
	}

	if (ioctl(socks[1], FIONBIO, &one) < 0) {
		close(socks[0]);
		close(socks[1]);
		return -1;
	}

	if ((pid = oct_fork()) == -1) {
		say_syserror("fork");
		close(socks[0]);
		close(socks[1]);
		return -1;
	}

	if (pid) {
		close(socks[0]);
		*sock = socks[1];
		return pid;
	} else {
		close(socks[1]);
		*sock = socks[0];
		return pid;
	}
}

static int spawner_fd = -1;

ssize_t
sendfd(int sock, int fd_to_send, void *buf, size_t buflen)
{
	char cmsgbuf[CMSG_SPACE(sizeof(int))];
	struct iovec iov = { .iov_base = buf,
			     .iov_len = buflen };
	struct msghdr msg = { .msg_iov = &iov,
			      .msg_iovlen = 1 };

	if (fd_to_send >= 0) {
		msg.msg_control = cmsgbuf;
		msg.msg_controllen = CMSG_LEN(sizeof(int));

		struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
		cmsg->cmsg_len = CMSG_LEN(sizeof(int));
		cmsg->cmsg_level = SOL_SOCKET;
		cmsg->cmsg_type = SCM_RIGHTS;
		*(int*)CMSG_DATA(cmsg) = fd_to_send;
	}

	ssize_t r;
	while ((r = sendmsg(sock, &msg, 0)) < 0) {
		say_syserror("sendmsg");
		if (errno != EAGAIN || errno != EINTR)
			break;
	}
	return r;
}

ssize_t
recvfd(int sock, int *fd, void *buf, size_t buflen)
{
	struct iovec iov = { .iov_base = buf,
			     .iov_len = buflen };
	char fdbuf[CMSG_SPACE(sizeof(int))];
	struct msghdr msg = { .msg_iov = &iov,
			      .msg_iovlen = 1,
			      .msg_control = fdbuf,
			      .msg_controllen = sizeof(fdbuf) };
	ssize_t r;
	while ((r = recvmsg(sock, &msg, 0)) < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
			continue;
		say_syserror("recvmsg");
		return -1;
	}
	if (r == 0) {
		say_error("recvmsg: EOF");
		return -1;
	}

	struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
	memcpy(fd, CMSG_DATA(cmsg), sizeof(int));
	return r;
}

static void
flush_and_exit(void)
{
	fflush(NULL);
	_exit(0);
}

int
fork_spawner()
{
	int fsock;
	pid_t pid;

	assert(fiber == sched);

	pid = fork_pair(SOCK_SEQPACKET, &fsock);
	if (pid < 0)
		return -1;

	if (pid > 0) {
		spawner_fd = fsock;
		return pid;
	}

#if OCT_CHILDREN
	extern int keepalive_pipe[2];
	close_all_xcpt(4, fsock, stderrfd, sayfd, keepalive_pipe[1]);
#else
	close_all_xcpt(3, fsock, stderrfd, sayfd);
#endif
	fiber = sched = [Fiber alloc];
	sched->name = "spawner";
	title("");
	say_info("spawner started");

	struct sigaction sa;
	memset(&sa, 0, sizeof(sa));
	sigemptyset(&sa.sa_mask);
	sa.sa_handler = SIG_IGN;
	if (sigaction(SIGCHLD, &sa, 0) == -1)
		say_syserror("sigaction");

	for (;;) {
		struct fork_request request;
		char buf[512 * 1024];
		struct iovec iov[2] = { { .iov_base = &request,
					  .iov_len = sizeof(request) },
					{ .iov_base = buf,
					  .iov_len = sizeof(buf) } };
		struct msghdr msg = { .msg_iov = iov,
				      .msg_iovlen = 2 };


		ssize_t len = recvmsg(fsock, &msg, 0);
		if (len == 0 || len < 0 || len == sizeof(request) + sizeof(buf)) {
			if (len < 0)
				say_syserror("recvmsg");
			if (len == sizeof(request) + sizeof(buf))
				say_error("spawner: request buffer overflow");
			break;
		}

		int sock = -1;
		pid = fork_pair(SOCK_STREAM, &sock);
		if (pid != 0) {
			struct fork_reply reply = { .pid = pid };
			if (sendfd(fsock, sock, &reply, sizeof(reply)) < 0)
				_exit(0);
			close(sock);
		} else {
			close(fsock);
			octopus_ev_init();
			fiber_init(request.name);
			luaT_init();
			sched->name = request.name;
			title("");
			say_info("%s spawned", request.name);
#ifdef HAVE_LIBELF
			struct symbol *sym = addr2symbol(request.handler);
			say_debug("worker %p:%s(fd:%d)",
				  request.handler, sym ? sym->name : "(unknown)",
				  sock);
#endif
			atexit(flush_and_exit);
			int rc = request.handler(sock, buf, len - sizeof(request));
			fflush(NULL); /* safe, because all inherited fd's were closeed */
			_exit(rc);
		}
	}
	_exit(0);
}

static void
io_ready(int event)
{
	ev_io io = { .coro = 1 };

	ev_io_init(&io, (void *)fiber, spawner_fd, event);
	ev_io_start(&io);
	yield();
	ev_io_stop(&io);
}

struct child
spawn_child(const char *name, int (*handler)(int fd, void *state, int len), void *state, int len)
{
	struct child err = { .pid = -1, .fd = -1};
	int fd = -1;
	pid_t pid;

	assert(spawner_fd != -1);
	struct fork_request request = { .handler = handler };
	assert(strlen(name) < sizeof(request.name));
	strcpy(request.name, name);

	struct iovec iov[2] = { { .iov_base = &request,
				  .iov_len = sizeof(request) },
				{ .iov_base = state,
				  .iov_len = len } };
	struct msghdr msg = { .msg_iov = iov,
			      .msg_iovlen = 2 };

	if (fiber != sched)
		io_ready(EV_WRITE);
	while (sendmsg(spawner_fd, &msg, 0) !=  sizeof(request) + len) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
			continue;
		say_syserror("sendmsg");
		return err;
	}

	struct fork_reply reply;

	if (fiber != sched)
		io_ready(EV_READ);

	if (recvfd(spawner_fd, &fd, &reply, sizeof(reply)) < 0)
		return err;

	/* fd is in non blocking mode, fork_pair() already did ioctl() */

	pid = reply.pid;
	if (pid <= 0) {
		say_error("unable to spawn %s", name);
		return err;
	}

	assert(fd >= 0);
	return (struct child){ .pid = pid, .fd = fd};
}
