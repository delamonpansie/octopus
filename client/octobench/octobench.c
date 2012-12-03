/*
 * Copyright (C) 2012 Mail.RU
 * Copyright (C) 2012 Teodor Sigaev
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

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <math.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/time.h>
#include <poll.h>
#include <unistd.h>
#include <stdbool.h>
#include <stdint.h>
#include <inttypes.h>

/************************************************************************************/
/*-
 * Copyright (c) 1990, 1993
 *  The Regents of the University of California.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 4. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * Posix rand_r function added May 1999 by Wes Peters <wes@softweyr.com>.
 */

#define BL_RAND_MAX     0x7fffffff

static int
do_rand(unsigned long *ctx)
{
/*
 * Compute x = (7^5 * x) mod (2^31 - 1)
 * wihout overflowing 31 bits:
 *      (2^31 - 1) = 127773 * (7^5) + 2836
 * From "Random number generators: good ones are hard to find",
 * Park and Miller, Communications of the ACM, vol. 31, no. 10,
 * October 1988, p. 1195.
 */
	long hi, lo, x;

	/* Can't be initialized with 0, so use another value. */
	if (*ctx == 0)
		*ctx = 123459876;
	hi = *ctx / 127773;
	lo = *ctx % 127773;
	x = 16807 * lo - 2836 * hi;
	if (x < 0)
		x += 0x7fffffff;
	return ((*ctx = x) % ((unsigned long)BL_RAND_MAX + 1));
}

static int
blrand_r(unsigned int *ctx)
{
	unsigned long val = (unsigned long) *ctx;
	int r = do_rand(&val);

	*ctx = (unsigned int) val;
	return (r);
}

/************************************************************************************/

#include <client/libiproto/libiproto.h>
#include <iproto_def.h>
#define SUM_ERROR_CODES(x) LIBIPROTO_ERROR_CODES(x) ERROR_CODES(x)
enum li_error_codes ENUM_INITIALIZER(SUM_ERROR_CODES);

static u_int64_t	nWriteAhead = 10;
static u_int64_t	nRequests = 1000;
static u_int64_t	nActiveConnections = 10;
static u_int64_t	nConnections = 10;
static pthread_mutex_t	mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t	cond  = PTHREAD_COND_INITIALIZER;
static bool		ignoreFatal = false;

#define		OCTO_PING	(0xff00) /* XXX src/iproto.m:const uint32_t msg_ping = 0xff00; */
#define		BOX_INSERT	(13)
#define		BOX_SELECT	(17)

static u_int16_t	messageType = OCTO_PING;
static u_int32_t	minId = 0,
			maxId = 1000;


static 	char		*server = NULL;
int			port = -1;


typedef struct BenchRes {
	u_int32_t	nOk;
	u_int32_t	nProceed;
	double		maxTime;
	double		minTime;
	double		totalTime;
	u_int32_t	errstat[256];
} BenchRes;

static BenchRes SumResults;

static void
initBenchRes(BenchRes *res) {
	memset(res, 0, sizeof(*res));
	res->minTime = 1e10;
}

static void
signalMain(BenchRes *local) {
	int	i;

	pthread_mutex_lock(&mutex);

	nActiveConnections--;

	SumResults.nOk += local->nOk;
	SumResults.nProceed += local->nProceed;

	if (local->maxTime > SumResults.maxTime)
		SumResults.maxTime = local->maxTime;
	if (local->minTime < SumResults.minTime)
		SumResults.minTime = local->minTime;
	SumResults.totalTime += local->totalTime;

	for(i=0; i<256; i++)
		SumResults.errstat[i] += local->errstat[i]; 

	pthread_cond_signal(&cond);
	pthread_mutex_unlock(&mutex);
}

static void*
my_alloc(void *ptr, size_t size) {
	if (ptr == NULL)
		return malloc(size);
	if (size == 0) {
		free(ptr);
		return NULL;
	}
	return realloc(ptr, size);
}

static int
octopoll(int fd, int event) {
	struct pollfd	pfd;
	int		ret;

	pfd.fd = fd;
	pfd.events = event;
	pfd.revents = 0;

	ret = poll(&pfd, 1, -1);
	if (ret < 0 || (pfd.revents & (POLLHUP | POLLNVAL | POLLERR)) != 0)
		return (pfd.revents | POLLERR);

	return pfd.revents;
}

static inline double
timediff(struct timeval *begin, struct timeval *end) {
    return ((double)( end->tv_sec - begin->tv_sec )) + ( (double)( end->tv_usec-begin->tv_usec ) ) / 1.0e+6;
}

typedef struct AllocatedRequestBody {
	struct AllocatedRequestBody	*next;
	struct timeval			begin; 
	/* data follows */
} AllocatedRequestBody;

static inline void *
popAllocatedRequestBody(AllocatedRequestBody **stack, size_t size, struct timeval *begin) {
	AllocatedRequestBody	*p;

	if (*stack) {
		p = *stack;
		*stack = (*stack)->next;
	} else {
		p = malloc(size + sizeof(*p));
	}

	p->begin = *begin;

	return ((char*)p) + sizeof(*p);
}

static inline struct timeval*
pushAllocatedRequestBody(AllocatedRequestBody **stack, char *ptr) {

	if (ptr) {
		AllocatedRequestBody	*p;

		p = (AllocatedRequestBody*)(ptr - sizeof(*p));
		p->next = *stack;
		*stack = p;

		return &p->begin;
	} 

	return NULL;
}

static inline void*
generateRequestBody(AllocatedRequestBody **stack, unsigned int *seed, size_t *size, struct timeval *begin) {
	void *ptr = NULL;

	*size = 0;

#define GEN_RND_BETWEEN(mn, mx)	(mn + (mx - mn) * ( ((double)blrand_r(seed)) / ((double)BL_RAND_MAX) ))

	switch(messageType) {
		case OCTO_PING:
			ptr = popAllocatedRequestBody(stack, 0, begin);
			*size = 0;
			break;
		case BOX_INSERT:
			{
				struct {
					u_int32_t	object_space;
					u_int32_t	flags;
					u_int32_t	n;
					u_int8_t	len;
					u_int32_t	id;
				} __attribute__((packed)) *ps, s = {0, 0, 1, 4, 0};

				ps = popAllocatedRequestBody(stack, sizeof(*ps), begin);
				memcpy(ps, &s, sizeof(*ps));
				ps->id = GEN_RND_BETWEEN(minId, maxId);
				ptr = ps;
				*size = sizeof(*ps);
			}
			break;
		case BOX_SELECT:
			{
				struct {
					u_int32_t	object_space;
					u_int32_t	index;
					u_int32_t	offset;
					u_int32_t 	limit;
					u_int32_t	n;
					u_int32_t	cardinality;
					u_int8_t	len;
					u_int32_t	id;
				} __attribute__((packed)) *ps, s = {0, 0, 0, 0xffffffff, 1, 1, 4, 0};

				ps = popAllocatedRequestBody(stack, sizeof(*ps), begin);
				memcpy(ps, &s, sizeof(*ps));
				ps->id = GEN_RND_BETWEEN(minId, maxId);
				ptr = ps;
				*size = sizeof(*ps);
			}
			break;
		default:
			abort();
	}

	return ptr;
}

void*
worker(void *arg) {
	int				idWorker = (int)(uintptr_t)arg;
	unsigned int			rndseed = idWorker;
	struct memory_arena_pool_t*	rap = map_alloc(my_alloc, 2, 64*1024);
	struct memory_arena_pool_t*	reqap = map_alloc(my_alloc, 64, 64*1024);
	struct iproto_connection_t*	conn = li_conn_init(my_alloc, rap, reqap);
	u_int32_t			errcode;
	int				fd;
	u_int32_t			nSended = 0;
	bool				needToSend = false;
	AllocatedRequestBody		*stack = NULL;
	size_t				size;
	u_int32_t			flags = LIBIPROTO_OPT_NONBLOCK;
	BenchRes			local;

	if (messageType != OCTO_PING)
		flags |= LIBIPROTO_OPT_HAS_4BYTE_ERRCODE;

	while((errcode = li_connect(conn, server, port, flags)) == ERR_CODE_CONNECT_IN_PROGRESS)
		octopoll(li_get_fd(conn), POLLOUT);

	if (errcode != ERR_CODE_OK) {
		fprintf(stderr,"li_connect fails: %s (%08x)\n", errcode_desc(errcode), errcode);
		exit(1);
	}

	fd = li_get_fd(conn);
	initBenchRes(&local);

	while(!(local.nOk >= nRequests && local.nProceed == nSended)) {
		int state;

		state = octopoll(fd, POLLIN | ((needToSend || local.nOk < nRequests) ? POLLOUT : 0));
		if (state & POLLERR) {
			fprintf(stderr,"poll fails: %s\n", strerror(errno));
			exit(1);
		}

		if (state & POLLIN) {
			struct iproto_request_t	*request;
			struct timeval *begin, end;
			double	elapsed;

			errcode = li_read(conn);

			if (!(errcode == ERR_CODE_NOTHING_TO_DO || errcode == ERR_CODE_OK)) {
				fprintf(stderr,"li_read fails: %s (%08x)\n", errcode_desc(errcode), errcode);
				exit(1);
			}

			gettimeofday(&end, NULL);
			while((request = li_get_ready_reqs(conn)) != NULL) {

				errcode = li_req_state(request);

				if (errcode != ERR_CODE_REQUEST_READY && (errcode & LIBIPROTO_ERR_CODE_FLAG)) {
					fprintf(stderr,"request fails: %s (%08x)\n", errcode_desc(errcode), errcode);
					exit(1);
				}

				if (errcode == ERR_CODE_OK || errcode == ERR_CODE_REQUEST_READY)
					local.nOk++;
				local.nProceed++;

				if (ignoreFatal == false && ERR_CODE_IS_FATAL(errcode)) {
					fprintf(stderr,"octopus returns fatal error: %s (%08x)\n",
						errcode_desc(errcode), errcode);
					exit(1);
				}

				local.errstat[ (errcode >> 8) & 0xff ] ++;

				begin = pushAllocatedRequestBody(&stack, li_req_request_data(request, &size));
				if (begin) {
					elapsed = timediff(begin, &end);

					if (elapsed > local.maxTime)
						local.maxTime = elapsed;
					if (elapsed < local.minTime)
						local.minTime = elapsed;
					local.totalTime += elapsed;
				}
				li_req_free(request);
			}
		}

		if (state & POLLOUT) {
			while (local.nOk < nRequests && (local.nProceed + nWriteAhead) > nSended) {
				int i;
				struct timeval begin;

				gettimeofday(&begin, NULL);

				for(i=0; i<nWriteAhead >> 2; i++) {
					void	*body;
					size_t	size;

					body = generateRequestBody(&stack, &rndseed, &size, &begin);
					li_req_init(conn, messageType, body, size);
				}

				nSended += nWriteAhead >> 2;

				needToSend = true;
			}

			if (needToSend) {
				switch((errcode = li_write(conn))) {
					case ERR_CODE_NOTHING_TO_DO:
						needToSend = false;
					case ERR_CODE_OK:
						break;
					default:
						fprintf(stderr,"li_write fails: %s (%08x)\n",
							errcode_desc(errcode), errcode);
						exit(1);
				}
			}
		}
	}

	signalMain(&local);
	li_close(conn);
	li_free(conn);
	map_free(rap);

	return NULL;
}

extern char *optarg;
extern int opterr, optind;

static void
usage(const char *errmsg) {
	/*    ################################################################################ */
	puts("octobench -s HOST -p PORT");
	puts("   [-n NREQUESTS] [-w NWRITE_AHEAD_REQUESTS] [-c NCONNECTION]");
	puts("   [-t (ping|box_insert|box_select)]");
	puts("   [-i MINID] [-I MAXID] -- min/max random id");
	puts("   [-F]                  -- ignore fatal error from db");
	puts("Defaults:");
	printf("    -n %"PRIu64" -w %"PRIu64" -c %"PRIu64"\n    -t ping", nRequests, nWriteAhead, nConnections);
	printf("    -i %u -I %u\n", minId, maxId);
	if (errmsg) {
		puts("");
		puts(errmsg);
	}
	exit(1);
}

#define	OPT_ERR(cond)	if (!(cond))	{						\
	fprintf(stderr, "Input option check fails: " #cond "\n");	\
	exit(1);													\
}

int
main(int argc, char* argv[]) {
	int 			i;
	struct timeval 		begin, end;
	double			elapsed;
	pthread_t		tid;

	nConnections = nActiveConnections;

	while((i=getopt(argc,argv,"s:p:n:w:c:t:Fi:I:h")) != EOF) {
		switch(i) {
			case 's':
				server = strdup(optarg);
				break;
			case 'p':
				port = atoi(optarg);
				break;
			case 'w':
				nWriteAhead = atoi(optarg);
				break;
			case 'n':
				nRequests = atoi(optarg);
				break;
			case 'c':
				nActiveConnections = nConnections = atoi(optarg);
				break;
			case 't':
				if (strcmp("ping", optarg) == 0)
					messageType = OCTO_PING;
				else if (strcmp("box_insert", optarg) == 0)
					messageType = BOX_INSERT;
				else if (strcmp("box_select", optarg) == 0)
					messageType = BOX_SELECT;
				else
					usage("error: unknown type");
				break;
			case 'i':
				minId = strtoul(optarg, NULL, 0);
				break;
			case  'I':
				maxId = strtoul(optarg, NULL, 0);
				break;
			case 'F':
				ignoreFatal = true;
				break;
			case 'h':
			default:
				usage(NULL);
		}
	}

	OPT_ERR(nWriteAhead <= nRequests);
	OPT_ERR(nWriteAhead >= 4);
	OPT_ERR(optind == argc);
	OPT_ERR(minId < maxId);

	if (server==NULL || port <= 0)
		usage("error: bad server address/port");

	ERRCODE_ADD(ERRCODE_DESCRIPTION, ERROR_CODES);

	initBenchRes(&SumResults);

	gettimeofday(&begin,NULL);
	pthread_mutex_lock(&mutex);
	for(i=0; i<nConnections; i++) {
		int err;

		if ((err = pthread_create(&tid, NULL, worker, (void*)(uintptr_t)(i+1))) != 0) {
			printf("pthread_create fails: %s\n", strerror(err));
			exit(1);
		}
	}

	while(nActiveConnections > 0)
		pthread_cond_wait(&cond, &mutex);
	pthread_mutex_unlock(&mutex);
	gettimeofday(&end,NULL);

	elapsed = timediff(&begin, &end);

	printf("Elapsed time: %.3f secs\n", elapsed);
	printf("Number of OK requests: %.3f\n", ((double)SumResults.nOk));
	printf("RPS: %.3f\n", ((double)SumResults.nOk) / elapsed );
	printf("Number of ALL requests: %.3f\n", ((double)SumResults.nProceed));
	printf("RPS: %.3f\n", ((double)SumResults.nProceed) / elapsed );
	printf("MIN/AVG/MAX time per request: %.03f / %.03f / %.03f millisecs\n", 
	       SumResults.minTime * 1e3, 
	       SumResults.totalTime * 1e3/ (double)SumResults.nProceed, 
	       SumResults.maxTime * 1e3);  

	for(i=0; i<256; i++)
		if (SumResults.errstat[i] > 0)
			printf("N number of %02x: %d\n", i, SumResults.errstat[i]
			       /* errcode_desc(i): i is only one byte from error code */);

	return 0;
}
