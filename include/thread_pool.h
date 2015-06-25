#ifndef _THREAD_POOL_H_
#define _THREAD_POOL_H_

#include "util.h"
#include <pthread.h>
#include <limits.h>

typedef union _request_arg {
	i64 i;
	u64 u;
	struct {
		union { i32 i; u32 u; } a;
		union { i32 i; u32 u; } b;
	} s;
	void* p;
} request_arg;

typedef void (*thread_callback)(request_arg cb_arg, i64 res, id error);

typedef struct _thread_request {
	thread_callback cb;
	request_arg arg;
	request_arg cb_arg;
} thread_request;

typedef struct _thread_response {
	thread_callback cb;
	request_arg cb_arg;
	i64 result;
	id  error;
	int eno;
} thread_response;

typedef struct _thread_response_internal {
	i64 result;
	id  error;
	int eno;
} thread_response_internal;

typedef struct _thread_pool_request {
	struct _thread_pool_request *next;
	thread_request req;
	thread_response_internal res;
} thread_pool_request;

typedef struct _thread_pool_waiter {
	pthread_mutex_t mtx;
	pthread_cond_t  cnd;
	thread_pool_request *req;
	struct _thread_pool_waiter* next;
} thread_pool_waiter;

typedef struct _thread_requests {
	pthread_mutex_t mtx;
	thread_pool_request *first;
	thread_pool_request *last;
	thread_pool_waiter *waiter;
} thread_requests;

void thread_requests_init(thread_requests *queue);
void thread_requests_finalize(thread_requests *queue);
void thread_requests_send(thread_requests* queue, thread_request req);
thread_pool_request* thread_requests_pop(thread_requests* queue, thread_pool_waiter* waiter);
void thread_fill_waittill(struct timespec *ts, double seconds);
thread_pool_request* thread_requests_pop_waittill(thread_requests* queue, struct timespec *timeout, thread_pool_waiter* waiter);

typedef struct _thread_responses {
	int ifd, ofd;
	thread_pool_request *first;
	volatile thread_pool_request *last;
	ev_io ev;
	int ev_started;
} thread_responses;

void thread_responses_init(thread_responses *queue);
void thread_responses_finalize(thread_responses *queue);
void thread_responses_push(thread_responses *queue, thread_pool_request *request, thread_response_internal res);
void thread_responses_callbacks_fiber_loop(va_list va);
/* should be called always from a same fiber */
int thread_responses_fiber_wait(thread_responses *queue);
int thread_responses_wait(thread_responses *queue, double seconds);
int thread_responses_possibly_have(thread_responses *queue);
int thread_responses_get(thread_responses *queue, thread_response *res);

#define THREAD_POOL_REQUEST_TIMEDOUT ((thread_pool_request*)(uintptr_t)1)

@interface ThreadWorker : Object {
@public
	int threadn;         /* number of threads */
	pthread_t *threads;  /* array of threads */
	thread_requests requests;
};

- (id) init_num: (int) n;
- (void) send_req: (thread_request)req;
- (void) send: (request_arg)arg;
- (void) send: (request_arg)arg cb: (thread_callback)cb cb_arg: (request_arg)cb_arg;
/* Specialization api */
- (void) thread_loop: (thread_pool_waiter*) waiter;
/* called by default thread_loop */
- (i64) perform: (request_arg)arg;
- (void) close;
/* Implementation api */
- (thread_pool_request*) pop_request: (thread_pool_waiter*) w;
- (thread_pool_request*) pop_timeout: (double)seconds waiter: (thread_pool_waiter*) w;
- (thread_pool_request*) pop_till: (struct timespec*)seconds waiter: (thread_pool_waiter*) w;
@end

@interface ThreadPool : ThreadWorker {
@public
	thread_responses responses;
	Fiber *resrdr; /* response reader */
};

- (i64) call: (request_arg)arg;
/* Implementation api */
- (void) respond: (thread_pool_request*)request res: (i64)res;
- (void) respond: (thread_pool_request*)request error: (id)e;
@end

#endif
