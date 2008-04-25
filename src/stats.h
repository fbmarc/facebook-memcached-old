/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#if !defined(_stats_h_)
#define _stats_h

/* stats */
extern void stats_prefix_init(void);
extern void stats_prefix_clear(void);
extern void stats_prefix_record_get(const char *key, const bool is_hit);
extern void stats_prefix_record_delete(const char *key);
extern void stats_prefix_record_set(const char *key);
extern void stats_prefix_record_byte_total_change(char *key, long bytes);
extern void stats_prefix_record_removal(char *key, size_t bytes, rel_time_t time, long flags);

/*@null@*/
extern char *stats_prefix_dump(int *length);

#define BUCKETS_RANGE(start, end, skip)  uint64_t   size_ ## start ## _ ## end [ ((end-start) / skip) ];
typedef struct _size_buckets SIZE_BUCKETS;
struct _size_buckets {
#include "buckets.h"
};

extern SIZE_BUCKETS set;
extern SIZE_BUCKETS get;
extern SIZE_BUCKETS evict;
extern SIZE_BUCKETS delete;
extern SIZE_BUCKETS overwrite;

/* stats size buckets */
extern void stats_buckets_init(void);

static inline void stats_size_buckets_set(size_t sz) {
#if defined(STATS_BUCKETS)
#define BUCKETS_RANGE(start, end, skip) if (sz >= start && sz < end) { set.size_ ## start ## _ ## end[ (sz - start) / skip ] ++; }
#include "buckets.h"
#endif /* #if defined(STATS_BUCKETS) */
}

static inline void stats_size_buckets_get(size_t sz) {
#if defined(STATS_BUCKETS)
#define BUCKETS_RANGE(start, end, skip) if (sz >= start && sz < end) { get.size_ ## start ## _ ## end[ (sz - start) / skip ] ++; }
#include "buckets.h"
#endif /* #if defined(STATS_BUCKETS) */
}

static inline void stats_size_buckets_evict(size_t sz) {
#if defined(STATS_BUCKETS)
#define BUCKETS_RANGE(start, end, skip) if (sz >= start && sz < end) { evict.size_ ## start ## _ ## end[ (sz - start) / skip ] ++; }
#include "buckets.h"
#endif /* #if defined(STATS_BUCKETS) */
}

static inline void stats_size_buckets_delete(size_t sz) {
#if defined(STATS_BUCKETS)
#define BUCKETS_RANGE(start, end, skip) if (sz >= start && sz < end) { delete.size_ ## start ## _ ## end[ (sz - start) / skip ] ++; }
#include "buckets.h"
#endif /* #if defined(STATS_BUCKETS) */
}

static inline void stats_size_buckets_overwrite(size_t sz) {
#if defined(STATS_BUCKETS)
#define BUCKETS_RANGE(start, end, skip) if (sz >= start && sz < end) { overwrite.size_ ## start ## _ ## end[ (sz - start) / skip ] ++; }
#include "buckets.h"
#endif /* #if defined(STATS_BUCKETS) */
}

extern char* item_stats_buckets(int *bytes);
#endif /* #if !defined(_stats_h_) */
