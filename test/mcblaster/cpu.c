#include <sys/param.h>
#include <sys/types.h>
#include <sys/time.h>
#ifdef __FreeBSD__
#include <sys/resource.h>
#include <sys/cpuset.h>
#else
#define __USE_GNU
#include <sched.h>
#endif
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <err.h>
#include "cpu.h"

static inline void
cpuid(void)
{
	uint32_t regs[4];
	uint32_t *p = regs;
	uint32_t ax = 0x0000001;

	__asm __volatile("cpuid"
	    : "=a" (p[0]), "=b" (p[1]), "=c" (p[2]), "=d" (p[3])
	    :  "0" (ax));
}

static long
get_us_interval(struct timeval *start, struct timeval *end)
{
	return (((end->tv_sec - start->tv_sec) * 1000000)
	    + (end->tv_usec - start->tv_usec));
}


double
get_cpu_frequency(void)
{
	struct timeval start;
	struct timeval end;
	uint64_t tsc_start;
	uint64_t tsc_end;
        long usec;

	if (gettimeofday(&start, 0))
		err(1, "gettimeofday");

	tsc_start = cycle_timer();
	usleep(10000);

	if (gettimeofday(&end, 0))
		err(1, "gettimeofday");
	tsc_end = cycle_timer();
        usec = get_us_interval(&start, &end);
	return (tsc_end - tsc_start) * 1.0 / usec;
}


#ifdef __FreeBSD__
void
bind_thread_to_cpu(int cpuid)
{
	cpuset_t mask;

	memset(&mask, 0, sizeof(mask));

	// bind this thread to a single cpu
	CPU_SET(cpuid, &mask);
	if (cpuset_setaffinity(CPU_LEVEL_WHICH, CPU_WHICH_TID, -1, sizeof(mask), &mask) < 0) {
		errx(1, "cpuset_setaffinity");
		return;
	}
}
#else
void
bind_thread_to_cpu(int cpuid)
{
	cpu_set_t mask;
	CPU_ZERO(&mask);

	CPU_SET(cpuid, &mask);
	if (sched_setaffinity(0, sizeof(cpu_set_t), &mask)) {
		err(1, "sched_setaffinity");
	}
}
#endif
