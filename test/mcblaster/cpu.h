#ifndef __CPU_H
#define __CPU_H
extern double get_cpu_frequency(void);
extern void bind_thread_to_cpu(int);

static inline uint64_t
cycle_timer(void)
{
	uint32_t __a,__d;
	uint64_t val;

	//cpuid();
	asm volatile("rdtsc" : "=a" (__a), "=d" (__d));
	(val) = ((uint64_t)__a) | (((uint64_t)__d)<<32);
	return val;
}

static inline double
get_microseconds(double cpu_freq)
{
	return cycle_timer() / cpu_freq;
}


static inline uint64_t
get_microsecond_from_tsc(uint64_t count, double cpu_frequency)
{
	return count / cpu_frequency;
}


#endif
