/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Slabs memory allocation, based on powers-of-N. Slabs are up to 1MB in size
 * and are divided into chunks. The chunk sizes start off at the size of the
 * "item" structure plus space for a small key and value. They increase by
 * a multiplier factor from there, up to half the maximum slab size. The last
 * slab size is always 1MB, since that's the maximum item size allowed by the
 * memcached protocol.
 *
 * $Id: slabs.c 352 2006-09-04 10:41:36Z bradfitz $
 */
#include "memcached.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#define POWER_SMALLEST 1
#define POWER_LARGEST  200
#define POWER_BLOCK 1048576
#define CHUNK_ALIGN_BYTES (sizeof(void *))
//#define DONT_PREALLOC_SLABS

/* powers-of-N allocation structures */

typedef struct {
    unsigned int size;      /* sizes of items */
    unsigned int perslab;   /* how many items per slab */

    void **slots;           /* list of item ptrs */
    unsigned int sl_total;  /* size of previous array */
    unsigned int sl_curr;   /* first free slot */

    void *end_page_ptr;         /* pointer to next free item at end of page, or 0 */
    unsigned int end_page_free; /* number of items remaining at end of last alloced page */

    unsigned int slabs;     /* how many slabs were allocated for this class */

    void **slab_list;       /* array of slab pointers */
    unsigned int list_size; /* size of prev array */

    unsigned int killing;  /* index+1 of dying slab, or zero if none */
    unsigned int total_hits;  /* total number of get hits for items in this slab class */
    unsigned int unique_hits; /* total number of get hits for unique items in this slab class */
    unsigned int evictions;   /* total number of evictions from this class */
    unsigned int rebalanced_to;
    unsigned int rebalanced_from;
    unsigned int rebalance_wait;
} slabclass_t;

static slabclass_t slabclass[POWER_LARGEST + 1];
static size_t mem_limit = 0;
static size_t mem_malloced = 0;
static int power_largest;
static int slab_rebalanced_count = 0;
static int slab_rebalanced_reversed = 0;

/*
 * Forward Declarations
 */
static int do_slabs_newslab(const unsigned int id);

#ifndef DONT_PREALLOC_SLABS
/* Preallocate as many slab pages as possible (called from slabs_init)
   on start-up, so users don't get confused out-of-memory errors when
   they do have free (in-slab) space, but no space to make new slabs.
   if maxslabs is 18 (POWER_LARGEST - POWER_SMALLEST + 1), then all
   slab types can be made.  if max memory is less than 18 MB, only the
   smaller ones will be made.  */
static void slabs_preallocate (const unsigned int maxslabs);
#endif

/*
 * Figures out which slab class (chunk size) is required to store an item of
 * a given size.
 *
 * Given object size, return id to use when allocating/freeing memory for object
 * 0 means error: can't store such a large object
 */

unsigned int slabs_clsid(const size_t size) {
    int res = POWER_SMALLEST;

    if (size == 0)
        return 0;
    while (size > slabclass[res].size)
        if (res++ == power_largest)     /* won't fit in the biggest slab */
            return 0;
    return res;
}

/**
 * Determines the chunk sizes and initializes the slab class descriptors
 * accordingly.
 */
void slabs_init(const size_t limit, const double factor) {
    int i = POWER_SMALLEST - 1;
    unsigned int size = sizeof(item) + settings.chunk_size;
    
    /* Factor of 2.0 means use the default memcached behavior */
    if (factor == 2.0 && size < 128)
        size = 128;

    mem_limit = limit;
    memset(slabclass, 0, sizeof(slabclass));

    while (++i < POWER_LARGEST && size <= POWER_BLOCK / 2) {
        /* Make sure items are always n-byte aligned */
        if (size % CHUNK_ALIGN_BYTES)
            size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);

        slabclass[i].size = size;
        slabclass[i].perslab = POWER_BLOCK / slabclass[i].size;
        size *= factor;
        if (settings.verbose > 1) {
            fprintf(stderr, "slab class %3d: chunk size %6u perslab %5u\n",
                    i, slabclass[i].size, slabclass[i].perslab);
        }
    }

    power_largest = i;
    slabclass[power_largest].size = POWER_BLOCK;
    slabclass[power_largest].perslab = 1;

    /* for the test suite:  faking of how much we've already malloc'd */
    {
        char *t_initial_malloc = getenv("T_MEMD_INITIAL_MALLOC");
        if (t_initial_malloc) {
            mem_malloced = (size_t)atol(t_initial_malloc);
        }

    }

#ifndef DONT_PREALLOC_SLABS
    {
        char *pre_alloc = getenv("T_MEMD_SLABS_ALLOC");

        if (pre_alloc == NULL || atoi(pre_alloc) != 0) {
            slabs_preallocate(power_largest);
        }
    }
#endif
}

#ifndef DONT_PREALLOC_SLABS
static void slabs_preallocate (const unsigned int maxslabs) {
    int i;
    unsigned int prealloc = 0;

    /* pre-allocate a 1MB slab in every size class so people don't get
       confused by non-intuitive "SERVER_ERROR out of memory"
       messages.  this is the most common question on the mailing
       list.  if you really don't want this, you can rebuild without
       these three lines.  */

    for (i = POWER_SMALLEST; i <= POWER_LARGEST; i++) {
        if (++prealloc > maxslabs)
            return;
        do_slabs_newslab(i);
    }

}
#endif

static int grow_slab_list (const unsigned int id) {
    slabclass_t *p = &slabclass[id];
    if (p->slabs == p->list_size) {
        size_t new_size =  (p->list_size != 0) ? p->list_size * 2 : 16;
        void *new_list = realloc(p->slab_list, new_size * sizeof(void *));
        if (new_list == 0) return 0;
        p->list_size = new_size;
        p->slab_list = new_list;
    }
    return 1;
}

static int do_slabs_newslab(const unsigned int id) {
    slabclass_t *p = &slabclass[id];
    int len = POWER_BLOCK;
    char *ptr;

    if (mem_limit && mem_malloced + len > mem_limit && p->slabs > 0)
        return 0;

    if (grow_slab_list(id) == 0) return 0;

    ptr = malloc((size_t)len);
    if (ptr == 0) return 0;

    memset(ptr, 0, (size_t)len);
    p->end_page_ptr = ptr;
    p->end_page_free = p->perslab;

    p->slab_list[p->slabs++] = ptr;
    mem_malloced += len;
    return 1;
}

/*@null@*/
void *do_slabs_alloc(const size_t size) {
    slabclass_t *p;

    unsigned int id = slabs_clsid(size);
    if (id < POWER_SMALLEST || id > power_largest)
        return NULL;

    p = &slabclass[id];
    assert(p->sl_curr == 0 || ((item *)p->slots[p->sl_curr - 1])->slabs_clsid == 0);

#ifdef USE_SYSTEM_MALLOC
    if (mem_limit && mem_malloced + size > mem_limit)
        return 0;
    mem_malloced += size;
    return malloc(size);
#endif

    /* fail unless we have space at the end of a recently allocated page,
       we have something on our freelist, or we could allocate a new page */
    if (! (p->end_page_ptr != 0 || p->sl_curr != 0 || do_slabs_newslab(id) != 0))
        return 0;

    /* return off our freelist, if we have one */
    if (p->sl_curr != 0)
        return p->slots[--p->sl_curr];

    /* if we recently allocated a whole page, return from that */
    if (p->end_page_ptr) {
        void *ptr = p->end_page_ptr;
        if (--p->end_page_free != 0) {
            p->end_page_ptr += p->size;
        } else {
            p->end_page_ptr = 0;
        }
        return ptr;
    }

    return NULL;  /* shouldn't ever get here */
}

void do_slabs_free(void *ptr, const size_t size) {
    unsigned char id = slabs_clsid(size);
    slabclass_t *p;

    assert(((item *)ptr)->slabs_clsid == 0);
    assert(id >= POWER_SMALLEST && id <= power_largest);
    if (id < POWER_SMALLEST || id > power_largest)
        return;

    p = &slabclass[id];

#ifdef USE_SYSTEM_MALLOC
    mem_malloced -= size;
    free(ptr);
    return;
#endif

    if (p->sl_curr == p->sl_total) { /* need more space on the free list */
        int new_size = (p->sl_total != 0) ? p->sl_total * 2 : 16;  /* 16 is arbitrary */
        void **new_slots = realloc(p->slots, new_size * sizeof(void *));
        if (new_slots == 0)
            return;
        p->slots = new_slots;
        p->sl_total = new_size;
    }
    p->slots[p->sl_curr++] = ptr;
    return;
}

/*@null@*/
char* do_slabs_stats(int *buflen) {
    int i, total;
    char *buf = (char *)malloc(power_largest * 1024 + 100);
    char *bufcurr = buf;

    *buflen = 0;
    if (buf == NULL) return NULL;

    total = 0;
    for(i = POWER_SMALLEST; i <= power_largest; i++) {
        slabclass_t *p = &slabclass[i];
        if (p->slabs != 0) {
            unsigned int perslab, slabs, used_chunks;

            slabs = p->slabs;
            perslab = p->perslab;
            used_chunks = slabs*perslab - p->sl_curr;
            double uhit = (double)p->unique_hits / slabs;
            double miss = (double)p->evictions * perslab;

            bufcurr += sprintf(bufcurr, "STAT %d:chunk_size %u\r\n", i, p->size);
            bufcurr += sprintf(bufcurr, "STAT %d:chunks_per_page %u\r\n", i, perslab);
            bufcurr += sprintf(bufcurr, "STAT %d:total_pages %u\r\n", i, slabs);
            bufcurr += sprintf(bufcurr, "STAT %d:total_chunks %u\r\n", i, slabs*perslab);
            bufcurr += sprintf(bufcurr, "STAT %d:used_chunks %u\r\n", i, used_chunks);
            bufcurr += sprintf(bufcurr, "STAT %d:free_chunks %u\r\n", i, p->sl_curr);
            bufcurr += sprintf(bufcurr, "STAT %d:free_chunks_end %u\r\n", i, p->end_page_free);
            bufcurr += sprintf(bufcurr, "STAT %d:total_items %u\r\n", i, used_chunks - p->end_page_free);
            bufcurr += sprintf(bufcurr, "STAT %d:total_hits %u\r\n", i, p->total_hits);
            bufcurr += sprintf(bufcurr, "STAT %d:unique_hits %u\r\n", i, p->unique_hits);
            bufcurr += sprintf(bufcurr, "STAT %d:evictions %u\r\n", i, p->evictions);
            bufcurr += sprintf(bufcurr, "STAT %d:uhits_per_slab %g\r\n", i, uhit);
            bufcurr += sprintf(bufcurr, "STAT %d:adjusted_evictions %g\r\n", i, miss);
            bufcurr += sprintf(bufcurr, "STAT %d:rebalanced_to %u\r\n", i, p->rebalanced_to);
            bufcurr += sprintf(bufcurr, "STAT %d:rebalanced_from %u\r\n", i, p->rebalanced_from);
            bufcurr += sprintf(bufcurr, "STAT %d:rebalance_wait %u\r\n", i, p->rebalance_wait);
            total++;
        }
    }
    bufcurr += sprintf(bufcurr, "STAT active_slabs %d\r\nSTAT total_malloced %llu\r\nSTAT total_rebalanced %d\r\nSTAT total_rebalance_reversed %d\r\n", total, (unsigned long long)mem_malloced, slab_rebalanced_count, slab_rebalanced_reversed);
    bufcurr += sprintf(bufcurr, "END\r\n");
    *buflen = bufcurr - buf;
    return buf;
}

/* Blows away all the items in a slab class and moves its slabs to another
   class. This is only used by the "slabs reassign" command, for manual tweaking
   of memory allocation.
   1 = success
   0 = fail
   -1 = tried. busy. send again shortly. */
int do_slabs_reassign(unsigned char srcid, unsigned char dstid) {
    void *slab, *slab_end;
    slabclass_t *p, *dp;
    void *iter;
    bool was_busy = false;

    if (srcid < POWER_SMALLEST || srcid > power_largest ||
        dstid < POWER_SMALLEST || dstid > power_largest ||
        srcid == dstid)
        return 0;

    p = &slabclass[srcid];
    dp = &slabclass[dstid];

    /* fail if src still populating, or no slab to give up in src */
    if (p->end_page_ptr || ! p->slabs)
        return 0;

    /* fail if dst is still growing or we can't make room to hold its new one */
    if (dp->end_page_ptr || ! grow_slab_list(dstid))
        return 0;

    if (p->killing == 0) p->killing = 1;

    slab = p->slab_list[p->killing - 1];
    slab_end = (char*)slab + POWER_BLOCK;

    for (iter = slab; iter < slab_end; iter += p->size) {
        item *it = (item *)iter;
        if (it->slabs_clsid) {
            if (it->refcount) was_busy = true;
            do_item_unlink(it, UNLINK_IS_EVICT);
        }
    }

    /* go through free list and discard items that are no longer part of this slab */
    {
        int fi;
        for (fi = p->sl_curr - 1; fi >= 0; fi--) {
            if (p->slots[fi] >= slab && p->slots[fi] < slab_end) {
                p->sl_curr--;
                if (p->sl_curr > fi) p->slots[fi] = p->slots[p->sl_curr];
            }
        }
    }

    if (was_busy) return -1;

    /* if good, now move it to the dst slab class */
    p->slab_list[p->killing - 1] = p->slab_list[p->slabs - 1];
    p->slabs--;
    p->killing = 0;
    p->rebalanced_from++;
    dp->slab_list[dp->slabs++] = slab;
    dp->end_page_ptr = slab;
    dp->end_page_free = dp->perslab;
    dp->rebalanced_to++;
    /* this isn't too critical, but other parts of the code do asserts to
       make sure this field is always 0.  */
    for (iter = slab; iter < slab_end; iter += dp->size) {
        ((item *)iter)->slabs_clsid = 0;
    }
    return 1;
}

void slabs_add_hit(void *it, int unique) {
    slabclass_t *p = &slabclass[((item *)it)->slabs_clsid];
    p->total_hits++;
    if (unique) p->unique_hits++;
}

void slabs_add_eviction(unsigned int clsid) {
    slabclass[clsid].evictions++;
}

/**
 * Algorithm: It's all about deciding which slab to move from and which slab
 * to move to. These are rules and heuristics:
 *
 * 1. The sole goal of rebalancing is to reduce cache miss.
 *
 * 2. Which slab to move to: finding the most grumpy slab...
 *
 *    (1) Cache miss happens right before set_cache() is called.
 *    (2) A set_cache() may or may not trigger an eviction.
 *    (3) We don't care about a set_cache() that doesn't trigger an eviction,
 *        as giving this slab more memory won't help.
 *    (4) If a slab item size is small, giving one slab will reduce many many
 *        evictions.
 *
 *    Therefore, if a slab has high "adjusted eviction", it's a "grumpy" slab
 *    that we potentially need to give more memory to and that we potentially
 *    reduce evictions effectively.
 *
 *      adjusted eviction = eviction_count * items_per_slab
 *
 * 3. Which slab to move from: finding the most indifferent slab...
 *
 *    (1) Moving out slabs will potentially generate evictions from the class.
 *    (2) If a class has low hit rate, new evictions is less likely to create.
 *    (3) Unique hit is much better than total hit in deciding memory need.
 *    (4) Unique hit needs to be prorated by number of slabs a class has.
 *
 *    Therefore, if a slab has low "unique hit rate", it's an "indifferent"
 *    slab that doesn't care about taking away a slab.
 *
 *      unique hit rate = unique_hits / number_of_slabs
 *
 * 4. Is it the end of the story? No. There are slab classes that have high
 *    eviction items AND low hit rates. To avoid this problem, we count
 *    total number of evictions of both classes that were rebalanced between,
 *    then compare to find out whether a rebalance helped or not. If not, we
 *    reverse them (Bear Mountain-Climbing).
 *
 * 5. Sending slabs to jail: For high eviction + low hit slabs, we put
 *    a rebalance wait so to delay any rebalance of it and to give other slabs
 *    chances to be rebalanced.
 *
 */
void do_slabs_rebalance() {
    static int slab_from = 0;
    static int slab_to = 0;
    static double previous_eps = 0.0; // previous evictions per second
    static time_t counter_reset = 0;

    /* assess last rebalance's effect */
    if (slab_from && slab_to) {
        slabclass_t *p_from = &slabclass[slab_from];
        slabclass_t *p_to = &slabclass[slab_to];
        double eps;
        if (counter_reset == 0 || current_time == counter_reset) {
            eps = -1;
        } else {
            eps = (double)(p_from->evictions + p_to->evictions) /
                (current_time - counter_reset);
        }
        
        if (eps >= 0 && previous_eps >= 0 &&
            eps > (previous_eps * 105 / 100) /* 5% to avoid deviations */) {
            do_slabs_reassign(slab_to, slab_from); /* reverse them */
            slab_rebalanced_reversed++;
            slab_from = 0;
            slab_to = 0;
            p_to->rebalance_wait = 50;
            return;
        }
    }
    slab_from = 0;
    slab_to = 0;

    double highest_miss = 0.0;
    double lowest_uhit = 0.0;

    int i;
    int highest_inited = 0;
    int lowest_inited = 0;
    for (i = POWER_SMALLEST; i <= power_largest; i++) {
        slabclass_t *p = &slabclass[i];
        /* we only consider classes that are full */
        if (!p->slabs || p->end_page_ptr) continue;
        if (p->rebalance_wait > 0) {
            p->rebalance_wait--;
            continue;
        }

        double uhit = (double)p->unique_hits / p->slabs;
        double miss = (double)p->evictions * p->perslab;

        if (!highest_inited) {
            highest_inited = 1;
            slab_to = i; highest_miss = miss;
        } else if (p->slabs > 1 && !lowest_inited) {
            lowest_inited = 1;
            slab_from = i; lowest_uhit = uhit;
        } else {
            if (miss > highest_miss) {
                slab_to = i; highest_miss = miss;
            }
            if (p->slabs > 1 && uhit < lowest_uhit) {
                slab_from = i; lowest_uhit = uhit;
            }
        }
    }

    if (slab_from && slab_to) {
        /* special case, we have high eviction items that are low hit */
        if (slab_from == slab_to) {
            slabclass_t *p = &slabclass[slab_from];
            slab_from = 0;
            slab_to = 0;
            p->rebalance_wait = 50;
            return;
        }

        if (do_slabs_reassign(slab_from, slab_to) == 1) {
            slabclass_t *p_from = &slabclass[slab_from];
            slabclass_t *p_to = &slabclass[slab_to];
            if (counter_reset == 0 || current_time == counter_reset) {
                previous_eps = -1;
            } else {
                previous_eps = (double)(p_from->evictions + p_to->evictions) /
                    (current_time - counter_reset);
            }

            /* reset all counts so we have new stats for next round */
            for (i = POWER_SMALLEST; i <= power_largest; i++) {
                slabclass_t *p = &slabclass[i];
                p->total_hits = 0;
                p->unique_hits = 0;
                p->evictions = 0;
            }
            counter_reset = current_time;
            slab_rebalanced_count++;
        } else {
            slab_from = slab_to = 0;
        }
    }
}
