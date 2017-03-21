#include <config.h>
#define malloc xmalloc
#ifndef THREADS
#define __thread
#endif
#define PALLOC_CHUNK_TTL 2048
#include <third_party/palloc/palloc.c>
