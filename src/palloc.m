#include <config.h>
#define malloc xmalloc
#ifndef THREADS
#define __thread
#endif
#include <third_party/palloc/palloc.c>
