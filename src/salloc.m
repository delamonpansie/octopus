#define SLAB_SIZE CFG_SLAB_SIZE
#include <third_party/salloc/salloc.c>
size_t CFG_SLAB_SIZE = 1 << 22;

#include <stat.h>
void slab_stat_report_cb(int base _unused_)
{
	slab_stat_report(stat_report_gauge);
}
