/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#if !defined(_stats_h_)
#define _stats_h

#include <assert.h>

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

#if defined(STATS_BUCKETS)
#define BUCKETS_RANGE(start, end, skip)  uint64_t   size_ ## start ## _ ## end [ ((end-start) / skip) ];
typedef struct _size_buckets SIZE_BUCKETS;
struct _size_buckets {
#include "buckets.h"
};

extern SIZE_BUCKETS set;
extern SIZE_BUCKETS hit;
extern SIZE_BUCKETS evict;
extern SIZE_BUCKETS delete;
extern SIZE_BUCKETS overwrite;
extern SIZE_BUCKETS expires;
#endif /* #if defined(STATS_BUCKETS) */

#if defined(COST_BENEFIT_STATS)
#define BUCKETS_RANGE(start, end, skip)                                 \
    uint64_t   hits_ ## start ## _ ## end [ ((end-start) / skip) ];     \
    uint64_t   slot_seconds_ ## start ## _ ## end [ ((end-start) / skip) ]; \
    rel_time_t last_update_ ## start ## _ ## end [ ((end-start) / skip) ]; \
    uint32_t   slots_ ## start ## _ ## end [ ((end-start) / skip) ];
typedef struct cost_benefit_buckets_s cost_benefit_buckets_t;
struct cost_benefit_buckets_s {
#include "buckets.h"
};

extern cost_benefit_buckets_t cb_buckets;
#endif /* #if defined(COST_BENEFIT_STATS) */


/* stats size buckets */
extern void stats_buckets_init(void);
extern void stats_cost_benefit_init(void);

static inline void stats_set(size_t sz, size_t overwritten_sz) {
#if defined(STATS_BUCKETS)
    if (overwritten_sz != 0) {
#define BUCKETS_RANGE(start, end, skip)                                 \
        do {                                                            \
            if (overwritten_sz >= start && overwritten_sz < end) {      \
                overwrite.size_ ## start ## _ ## end[ (overwritten_sz - start) / skip ] ++; \
                break;                                                  \
            }                                                           \
        } while (0);
#include "buckets.h"
    }
#define BUCKETS_RANGE(start, end, skip)                                 \
    do {                                                                \
        if (sz >= start && sz < end) {                                  \
            set.size_ ## start ## _ ## end[ (sz - start) / skip ] ++;   \
            break;                                                      \
        }                                                               \
    } while (0);
#include "buckets.h"
#endif /* #if defined(STATS_BUCKETS) */

#if defined(COST_BENEFIT_STATS)
    {
        uint64_t* from_slot_seconds_ptr = NULL, * to_slot_seconds_ptr = NULL;
        rel_time_t* from_last_update_ptr = NULL, * to_last_update_ptr = NULL;
        uint32_t* from_slots_ptr = NULL, * to_slots_ptr = NULL;
        rel_time_t now = current_time;

        if (overwritten_sz != 0) {
#define BUCKETS_RANGE(start, end, skip)                                 \
            do {                                                        \
                if (overwritten_sz >= start && overwritten_sz < end) {  \
                    unsigned slot = (overwritten_sz - start) / skip;    \
                    from_slot_seconds_ptr = &cb_buckets.slot_seconds_ ## start ## _ ## end[slot]; \
                    from_last_update_ptr = &cb_buckets.last_update_ ## start ## _ ## end[slot]; \
                    from_slots_ptr = &cb_buckets.slots_ ## start ## _ ## end[slot]; \
                    break;                                              \
                }                                                       \
            } while (0);
#include "buckets.h"
        }

#define BUCKETS_RANGE(start, end, skip)                                 \
        do {                                                            \
            if (sz >= start && sz < end) {                              \
                unsigned slot = (sz - start) / skip;                    \
                to_slot_seconds_ptr = &cb_buckets.slot_seconds_ ## start ## _ ## end[slot]; \
                to_last_update_ptr = &cb_buckets.last_update_ ## start ## _ ## end[slot]; \
                to_slots_ptr = &cb_buckets.slots_ ## start ## _ ## end[slot]; \
                break;                                                  \
            }                                                           \
        } while (0);
#include "buckets.h"

        assert( (from_slot_seconds_ptr == NULL && from_last_update_ptr == NULL && from_slots_ptr == NULL) ||
                (from_slot_seconds_ptr != NULL && from_last_update_ptr != NULL && from_slots_ptr != NULL) );
        assert(to_slot_seconds_ptr != NULL);
        assert(to_last_update_ptr != NULL);
        assert(to_slots_ptr != NULL);

        if (from_slots_ptr != to_slots_ptr) {
            /* only need to do an update if the item has changed slots. */
            if (from_slots_ptr) {
                /* we are doing an overwrite, so refresh the from slot. */
                (*from_slot_seconds_ptr) += (*from_slots_ptr) * (now - *from_last_update_ptr);

                assert((*from_slots_ptr) > 0);
                (*from_slots_ptr) --;
                (*from_last_update_ptr) = now;
            }

            /* the to_slot is always updated */
            (*to_slot_seconds_ptr) += (*to_slots_ptr) * (now - *to_last_update_ptr);

            (*to_slots_ptr) ++;
            (*to_last_update_ptr) = now;

        }
    }
#endif /* #if defined(COST_BENEFIT_STATS) */
}

static inline void stats_get(size_t sz) {
#if defined(STATS_BUCKETS)
#define BUCKETS_RANGE(start, end, skip)                                 \
    do {                                                                \
        if (sz >= start && sz < end) {                                  \
            hit.size_ ## start ## _ ## end[ (sz - start) / skip ] ++;   \
            break;                                                      \
        }                                                               \
    } while (0);
#include "buckets.h"
#endif /* #if defined(STATS_BUCKETS) */

#if defined(COST_BENEFIT_STATS)
#define BUCKETS_RANGE(start, end, skip)                                 \
    do {                                                                \
        if (sz >= start && sz < end) {                                  \
            cb_buckets.hits_ ## start ## _ ## end[ (sz - start) / skip ] ++;   \
            break;                                                      \
        }                                                               \
    } while (0);
#include "buckets.h"
#endif /* #if defined(COST_BENEFIT_STATS) */
}

static inline void stats_evict(size_t sz) {
#if defined(STATS_BUCKETS)
#define BUCKETS_RANGE(start, end, skip)                                 \
    do {                                                                \
        if (sz >= start && sz < end) {                                  \
            evict.size_ ## start ## _ ## end[ (sz - start) / skip ] ++; \
            break;                                                      \
        }                                                               \
    } while (0);
#include "buckets.h"
#endif /* #if defined(STATS_BUCKETS) */

#if defined(COST_BENEFIT_STATS)
    {
        rel_time_t now = current_time;

#define BUCKETS_RANGE(start, end, skip)                                 \
        do {                                                            \
            if (sz >= start && sz < end) {                              \
                unsigned slot = (sz - start) / skip;                    \
                cb_buckets.slot_seconds_ ## start ## _ ## end[slot] +=  \
                    (now - cb_buckets.last_update_ ## start ## _ ## end [slot]) * \
                    cb_buckets.slots_ ## start ## _ ## end [slot];      \
                assert(cb_buckets.slots_ ## start ## _ ## end [slot] > 0); \
                cb_buckets.slots_ ## start ## _ ## end [slot] --;       \
                break;                                                  \
            }                                                           \
        } while (0);
#include "buckets.h"
    }
#endif /* #if defined(COST_BENEFIT_STATS) */
}

static inline void stats_delete(size_t sz) {
#if defined(STATS_BUCKETS)
#define BUCKETS_RANGE(start, end, skip)                                 \
    do {                                                                \
        if (sz >= start && sz < end) {                                  \
            delete.size_ ## start ## _ ## end[ (sz - start) / skip ] ++; \
            break;                                                      \
        }                                                               \
    } while (0);
#include "buckets.h"
#endif /* #if defined(STATS_BUCKETS) */

#if defined(COST_BENEFIT_STATS)
    {
        rel_time_t now = current_time;

#define BUCKETS_RANGE(start, end, skip)                                 \
        do {                                                            \
            if (sz >= start && sz < end) {                              \
                unsigned slot = (sz - start) / skip;                    \
                cb_buckets.slot_seconds_ ## start ## _ ## end[slot] +=  \
                    (now - cb_buckets.last_update_ ## start ## _ ## end [slot]) * \
                    cb_buckets.slots_ ## start ## _ ## end [slot];      \
                assert(cb_buckets.slots_ ## start ## _ ## end [slot] > 0); \
                cb_buckets.slots_ ## start ## _ ## end [slot] --;       \
                break;                                                  \
            }                                                           \
        } while (0);
#include "buckets.h"
    }
#endif /* #if defined(COST_BENEFIT_STATS) */
}

static inline void stats_expire(size_t sz) {
#if defined(STATS_BUCKETS)
#define BUCKETS_RANGE(start, end, skip)                                 \
    do {                                                                \
        if (sz >= start && sz < end) {                                  \
            expires.size_ ## start ## _ ## end[ (sz - start) / skip ] ++; \
            break;                                                      \
        }                                                               \
    } while (0);
#include "buckets.h"
#endif /* #if defined(STATS_BUCKETS) */

#if defined(COST_BENEFIT_STATS)
    {
        rel_time_t now = current_time;

#define BUCKETS_RANGE(start, end, skip)                                 \
        do {                                                            \
            if (sz >= start && sz < end) {                              \
                unsigned slot = (sz - start) / skip;                    \
                cb_buckets.slot_seconds_ ## start ## _ ## end[slot] +=  \
                    (now - cb_buckets.last_update_ ## start ## _ ## end [slot]) * \
                    cb_buckets.slots_ ## start ## _ ## end [slot];      \
                assert(cb_buckets.slots_ ## start ## _ ## end [slot] > 0); \
                cb_buckets.slots_ ## start ## _ ## end [slot] --;       \
                break;                                                  \
            }                                                           \
        } while (0);
#include "buckets.h"
    }
#endif /* #if defined(COST_BENEFIT_STATS) */
}

extern char* item_stats_buckets(int *bytes);
extern char* cost_benefit_stats(int *bytes);

#endif /* #if !defined(_stats_h_) */
