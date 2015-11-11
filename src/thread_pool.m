#import <util.h>
#import <fiber.h>
#import <log_io.h>
#import <index.h>
#import <say.h>
#include <stat.h>
#include <salloc.h>
#include <sysexits.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#ifdef HAVE_SYS_SYSCALL_H
#include <sys/syscall.h>
#elif defined(HAVE_SYSCALL_H)
#include <sys/syscall.h>
#endif
#include <unistd.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <string.h>
#include <octopus.h>
#include <time.h>
#include <poll.h>
#ifdef HAVE_EVENTFD
#include <sys/eventfd.h>
#endif

#include <pthread.h>

#import <objc.h>
#import <iproto.h>
#import "thread_pool.h"

#define pdo(m, ...) do { \
	int err = m(__VA_ARGS__); \
	if (err != 0) { \
		errno = err; \
		panic_syserror(#m); \
	} \
} while(0)

#if defined(CLOCK_MONOTONIC_RAW)
#define COND_CLOCK CLOCK_MONOTONIC_RAW
#elif defined(CLOCK_MONOTONIC)
#define COND_CLOCK CLOCK_MONOTONIC
#else
#define COND_CLOCK CLOCK_REALTIME
#endif
static int cond_clock = COND_CLOCK;

static void __attribute__((constructor))
init_cond_clock()
{
	struct timespec ts;
	if (!clock_gettime(cond_clock, &ts))
		cond_clock = CLOCK_REALTIME;
}

void
thread_fill_waittill(struct timespec *ts, double seconds)
{
	clock_gettime(cond_clock, ts);
	time_t sec = seconds;
	ts->tv_nsec += (seconds - sec) * 1e9;
	ts->tv_sec += sec;
	if (ts->tv_nsec >= 1e9) {
		ts->tv_nsec -= 1e9;
		ts->tv_sec++;
	}
}

void
thread_requests_init(thread_requests *queue)
{
	thread_pool_request *thumb;
	thumb = xcalloc(1, sizeof(*thumb));
	queue->first = queue->last = thumb;
	pdo(pthread_mutex_init, &queue->mtx, NULL);
}

void
thread_requests_finalize(thread_requests *queue)
{
	pdo(pthread_mutex_destroy, &queue->mtx);
	thread_pool_request *p, *r = (typeof(r))queue->first;
	while(r) {
		p = (typeof(p))r->next;
		free(r);
		r = p;
	}
}

void
thread_requests_send(thread_requests* queue, thread_request req)
{
	thread_pool_request *thumb, *last, *first;
	thumb = xcalloc(1, sizeof(*thumb));
	last = queue->last;
	last->req = req;
	zero_io_collect_interval();
	pdo(pthread_mutex_lock, &queue->mtx);
	last->next = thumb;
	queue->last = thumb;
	while (queue->waiter != NULL && queue->first != queue->last) {
		thread_pool_waiter* waiter = queue->waiter;
		queue->waiter = waiter->next;
		first = queue->first;
		assert(first->next != NULL);
		queue->first = (__typeof__(queue->first))first->next;
		pdo(pthread_mutex_lock, &waiter->mtx);
		waiter->next = NULL;
		waiter->req = first;
		pdo(pthread_mutex_unlock, &waiter->mtx);
		pdo(pthread_cond_signal, &waiter->cnd);
	}
	pdo(pthread_mutex_unlock, &queue->mtx);
}

thread_pool_request*
thread_requests_pop(thread_requests* queue, thread_pool_waiter* waiter)
{
	thread_pool_request *request = NULL;
	pdo(pthread_mutex_lock, &queue->mtx);
	if (queue->first->next != NULL) {
		request = (thread_pool_request*)queue->first;
		queue->first = (__typeof__(queue->first))request->next;
		pdo(pthread_mutex_unlock, &queue->mtx);
	} else {
		assert(waiter->next == NULL);
		waiter->next = queue->waiter;
		queue->waiter = waiter;
		assert(waiter->req == NULL);
		pdo(pthread_mutex_unlock, &queue->mtx);
		pdo(pthread_mutex_lock, &waiter->mtx);
		while (waiter->req == NULL)
			pdo(pthread_cond_wait, &waiter->cnd, &waiter->mtx);
		pdo(pthread_mutex_unlock, &waiter->mtx);
		assert(waiter->next == NULL);
		request = waiter->req;
		waiter->req = NULL;
	}
	return request;
}

thread_pool_request*
thread_requests_pop_waittill(thread_requests* queue, struct timespec *timeout, thread_pool_waiter* waiter)
{
	thread_pool_request *request = NULL;
	pdo(pthread_mutex_lock, &queue->mtx);
	if (queue->first->next != NULL) {
		request = (thread_pool_request*)queue->first;
		queue->first = (__typeof__(queue->first))request->next;
		pdo(pthread_mutex_unlock, &queue->mtx);
	} else {
		assert(waiter->next == NULL);
		waiter->next = queue->waiter;
		queue->waiter = waiter;
		assert(waiter->req == NULL);
		pdo(pthread_mutex_unlock, &queue->mtx);
		pdo(pthread_mutex_lock, &waiter->mtx);
		while (waiter->req == NULL) {
			int err = pthread_cond_timedwait(&waiter->cnd, &waiter->mtx, timeout);
			if (err != 0) {
				if (err == ETIMEDOUT) {
					pdo(pthread_mutex_unlock, &waiter->mtx);
					pdo(pthread_mutex_lock, &queue->mtx);
					request = waiter->req;
					if (request == NULL) {
						thread_pool_waiter **w = &queue->waiter;
						while (*w != waiter) {
							w = &(*w)->next;
						}
						*w = waiter->next;
						waiter->next = NULL;
					}
					pdo(pthread_mutex_unlock, &queue->mtx);
					assert(waiter->next == NULL);
					return request;
				}
				errno = err;
				panic_syserror("pthread_cond_timedwait");
			}
		}
		pdo(pthread_mutex_unlock, &waiter->mtx);
		request = waiter->req;
		waiter->req = NULL;
	}
	return request;
}

void
thread_responses_init(thread_responses *queue)
{
	thread_pool_request *thumb;
	thumb = xcalloc(1, sizeof(*thumb));
	queue->last = queue->first = thumb;
#ifdef HAVE_EVENTFD
	queue->ifd = queue->ofd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	if (queue->ifd < 0 && errno == EINVAL) {
		queue->ifd = queue->ofd = eventfd(0, 0);
		if (queue->ifd < 0)
			panic_syserror("eventfd");
		fcntl(queue->ifd, F_SETFD, FD_CLOEXEC);
		fcntl(queue->ifd, F_SETFL, O_NONBLOCK);
	}
#else
	int fds[2];
	if (pipe(fds) == -1) {
		panic_syserror("pipe");
	};
	queue->ifd = fds[0]; queue->ofd = fds[1];
	fcntl(ifd, F_SETFD, FD_CLOEXEC);
	fcntl(ofd, F_SETFD, FD_CLOEXEC);
	fcntl(ifd, F_SETFL, O_NONBLOCK);
#endif
}

void
thread_responses_finalize(thread_responses *queue)
{
	close(queue->ifd);
	if (queue->ofd != queue->ifd)
		close(queue->ofd);
}

void
thread_responses_push(thread_responses *queue, thread_pool_request *request, thread_response_internal res)
{
	thread_pool_request *prev;
	request->res = res;
	request->next = NULL;
	//prev = (typeof(prev))__sync_lock_test_and_set(&queue->last, request);
	prev = (typeof(prev))__atomic_exchange_n(&queue->last, request, __ATOMIC_ACQ_REL);
	__atomic_store_n(&prev->next, request, __ATOMIC_RELEASE);
#ifdef HAVE_EVENTFD
	u64 v = 1;
#else
	u8 v = 1;
#endif
retry:
	if (write(queue->ofd, &v, sizeof(v)) == -1) {
		if (errno == EINTR)
			goto retry;
		panic_syserror("write(eventfd)");
	}
}

int
thread_responses_possibly_have(thread_responses *queue)
{
#ifdef HAVE_EVENTFD
	char buf[8];
#else
	char buf[2048];
#endif
	ssize_t n = read(queue->ifd, buf, sizeof(buf));
	if (n == -1) {
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return 0;
		panic_syserror("read(queue->ifd)");
	}
#ifdef HAVE_EVENTFD
	return (int)(*(u64*)buf);
#else
	return (int)n;
#endif
}

int
thread_responses_fiber_wait(thread_responses *queue)
{
	if (!queue->ev_started) {
		queue->ev_started = 1;
		queue->ev.coro = 1;
		ev_io_init(&queue->ev, (void*)fiber, queue->ifd, EV_READ);
		ev_set_priority(&queue->ev, 1);
		ev_io_start(&queue->ev);
	}
	yield();
	return thread_responses_possibly_have(queue);
}

int
thread_responses_wait(thread_responses *queue, double seconds)
{
	int r;
	struct pollfd pl = {.fd = queue->ifd, .events = POLLIN, .revents = 0};
	if (seconds * 1000 > 0x7fffffff) {
		seconds = 0x7fffffff / 1000;
	}
	r = poll(&pl, 1, seconds * 1000);
	if (r > 0) {
		if (pl.revents & POLLERR) {
			return 0;
		}
		return thread_responses_possibly_have(queue);
	} else if (r < 0) {
		panic_syserror("poll(queue->ifd)");
	} else {
		return 0;
	}
}

int
thread_responses_get(thread_responses *queue, thread_response *res)
{
	thread_pool_request *req, *next;
	req = (typeof(req))queue->first;
	next = (typeof(req))__atomic_load_n(&req->next, __ATOMIC_ACQUIRE);
	if (!next) {
		return 0;
	}
	queue->first = next;
	free(req);
	res->cb = next->req.cb;
	res->cb_arg = next->req.cb_arg;
	res->result = next->res.result;
	res->error = next->res.error;
	res->eno = next->res.eno;
	return 1;
}

extern ev_async wake_async;
void
thread_responses_callbacks_fiber_loop(va_list va)
{
	thread_responses *queue = va_arg(va, thread_responses*);
	thread_response res;
	for(;;) {
		int n = thread_responses_fiber_wait(queue);
		for(; n>0; --n) {
			/* spin lock to wait till ->next is set */
			for (;;) {
				bool were_get = thread_responses_get(queue, &res);
				if (were_get) break;
				ev_async_send(&wake_async);
				fiber_wake(fiber, NULL);
				yield();
			}
			errno = res.eno;
			res.cb(res.cb_arg, res.result, res.error);
			unzero_io_collect_interval();
		}
		fiber_gc();
	}
}

static void *
thread_loop(void *arg)
{
	char buf[24];
#ifdef SYS_gettid
	snprintf(buf, sizeof(buf)-1, "__thread:%d", (int)syscall(SYS_gettid));
#else
	static int thrd_cnt = 1;
	int thno = __sync_add_and_get(&thrd_cnt, 1);
	snprintf(buf, sizeof(buf)-1, "__thread:%d", thno);
#endif
	fiber_create_fake(buf);

	thread_pool_waiter waiter;
	memset(&waiter, 0, sizeof(waiter));
	pdo(pthread_mutex_init, &waiter.mtx, NULL);
	pthread_condattr_t cond_attr;
	pthread_condattr_init(&cond_attr);
	pthread_condattr_setclock(&cond_attr, cond_clock);
	pdo(pthread_cond_init, &waiter.cnd, &cond_attr);

	@try {
		[(ThreadWorker*)arg thread_loop: &waiter];
	} @finally {
		fiber_destroy_fake();
		pdo(pthread_mutex_destroy, &waiter.mtx);
		pdo(pthread_cond_destroy, &waiter.cnd);
	}
	return NULL;
}

@implementation ThreadWorker

- (id)
init_num: (int) n
{
	int thi, err;
	assert(n > 0);
	self = [super init];
	thread_requests_init(&requests);
	threadn = n;
	threads = xmalloc(sizeof(pthread_t) * n);
	for(thi=0; thi<n; thi++) {
		err = pthread_create(&threads[thi], NULL, thread_loop, self);
		if (err != 0) {
			errno = err;
			panic_syserror("pthread_create");
		}
	}
	return self;
}

- (i64)
perform_request: (request_arg)arg
{
	return arg.i;
}

- (void)
send_req: (thread_request)req
{
	thread_requests_send(&requests, req);
}

- (void)
send: (request_arg)arg
{
	thread_request req = {.arg = arg, .cb = (thread_callback)1, .cb_arg = arg};
	[self send_req: req];
}

- (void)
send: (request_arg)arg cb: (thread_callback)cb cb_arg: (request_arg)cb_arg
{
	thread_request req = {.arg = arg, .cb = cb, .cb_arg = cb_arg};
	[self send_req: req];
}

- (thread_pool_request*)
pop_request: (thread_pool_waiter*) waiter
{
	return thread_requests_pop(&requests, waiter);
}

- (thread_pool_request*)
pop_till: (struct timespec*)till waiter: (thread_pool_waiter*) waiter
{
	return thread_requests_pop_waittill(&requests, till, waiter);
}

- (thread_pool_request*)
pop_timeout: (double)seconds waiter: (thread_pool_waiter*) waiter
{
	struct timespec ts;
	thread_fill_waittill(&ts, seconds);
	return thread_requests_pop_waittill(&requests, &ts, waiter);
}

- (void)
thread_loop: (thread_pool_waiter*) waiter
{
	thread_pool_request *request = NULL;

	for(;;) {
		errno = 0;
		request = [self pop_request: waiter];
		if (request->req.cb == NULL) {
			return;
		}
		@try {
			[self perform_request: request->req.arg];
		}
		@catch (id e) {
			say_error("thread loop catched an error");
			[e release];
		}
		fiber_gc();
	}
}

- (void)
close
{
	int i;
	struct timespec timeout;
	thread_request req = {.arg = {.i = 0}, .cb = NULL, .cb_arg = {.i = 0}};
	for(i = 0; i < (threadn ? threadn : 1); i++) {
		[self send_req: req];
	}
	clock_gettime(CLOCK_REALTIME, &timeout);
	timeout.tv_sec += 2;
	for(i = 0; i < threadn; i++) {
		int err = pthread_timedjoin_np(threads[i], NULL, &timeout);
		if (err != 0) {
			if (err == ETIMEDOUT) {
				pdo(pthread_cancel, threads[i]);
			} else {
				errno = err;
				panic_syserror("pthread_timedjoin_np");
			}
		}
	}
}

@end

@implementation ThreadPool

- (id)
init_num: (int) n
{
	self = [super init_num: n];
	thread_responses_init(&responses);
	resrdr = fiber_create("thread_pool/reader", thread_responses_callbacks_fiber_loop, &self->responses);
	if (resrdr == NULL) {
		panic("could not create fiber");
	}
	return self;
}

struct call_request {
	struct Fiber *fib;
	i64 res;
	id error;
	int eno;
};

static void
call_callback(request_arg arg, i64 res, id error)
{
	struct call_request *cr = arg.p;
	cr->res = res;
	cr->error = error;
	cr->eno = errno;
	fiber_wake(cr->fib, NULL);
}

- (i64)
call: (request_arg)arg
{
	struct call_request cr = {.fib = fiber};
	thread_request req = {.arg = arg, .cb = call_callback, .cb_arg = {.p = &cr}};
	[self send_req: req];
	yield();
	errno = cr.eno;
	if (cr.error) {
		@throw cr.error;
	}
	return cr.res;
}

- (void)
respond: (thread_pool_request*)req res: (i64)res
{
	fiber_gc();
	thread_response_internal response = {.result = res, .error = nil, .eno = errno};
	thread_responses_push(&responses, req, response);
}

- (void)
respond: (thread_pool_request*)req error: (id)e
{
	fiber_gc();
	thread_response_internal response = {.result = 0, .error = e, .eno = errno};
	thread_responses_push(&responses, req, response);
}

- (void)
thread_loop: (thread_pool_waiter*)waiter
{
	thread_pool_request *request = NULL;
	i64 res;
	for(;;) {
		errno = 0;
		request = [self pop_request: waiter];
		if (request->req.cb == NULL) {
			[self respond: request res: 0];
			return;
		}
		@try {
			res = [self perform_request: request->req.arg];
			[self respond: request res: res];
		}
		@catch (id e) {
			say_error("thread loop catched an error");
			[self respond: request error: e];
		}
		fiber_gc();
	}
}

@end

register_source();
