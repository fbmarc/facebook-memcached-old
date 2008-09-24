/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Thread management for memcached.
 *
 *  $Id$
 */

#define _GNU_SOURCE 1
#include "generic.h"

#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <errno.h>
#include <pthread.h>

#include "memcached.h"
#include "assoc.h"
#include "items.h"
#include "stats.h"
#include "conn_buffer.h"

#define ITEMS_PER_ALLOC 64

/* An item in the connection queue. */
typedef struct conn_queue_item CQ_ITEM;
struct conn_queue_item {
    int     sfd;
    int     init_state;
    int     event_flags;
    int     is_udp;
    int     is_binary;
    struct sockaddr addr;
    socklen_t addrlen;
    conn_buffer_group_t* cbg;
    CQ_ITEM *next;
};

/* A connection queue. */
typedef struct conn_queue CQ;
struct conn_queue {
    CQ_ITEM *head;
    CQ_ITEM *tail;
    size_t count;
    pthread_mutex_t lock;
    pthread_cond_t  cond;
};

/* Lock for connection freelist */
static pthread_mutex_t conn_lock;

/* Lock for cache operations (item_*, assoc_*) */
static pthread_mutex_t cache_lock;
static pthread_mutexattr_t cache_attr;

#if defined(USE_SLAB_ALLOCATOR)
/* Lock for slab allocator operations */
static pthread_mutex_t slabs_lock;
#endif /* #if defined(USE_SLAB_ALLOCATOR) */

/* Lock for global stats */
static pthread_mutex_t gstats_lock;

/* Lock for global stats */
static pthread_mutex_t conn_buffer_lock;

/* Free list of CQ_ITEM structs */
static CQ_ITEM *cqi_freelist;
static pthread_mutex_t cqi_freelist_lock;

/*
 * Each libevent instance has a wakeup pipe, which other threads
 * can use to signal that they've put a new connection on its queue.
 */
typedef struct {
    pthread_t thread_id;        /* unique ID of this thread */
    struct event_base *base;    /* libevent handle this thread uses */
    struct event notify_event;  /* listen event for notify pipe */
    struct event timer_event;   /* periodic timer */
    bool timer_initialized;     /* timer is up and running */
    int notify_receive_fd;      /* receiving end of notify pipe */
    int notify_send_fd;         /* sending end of notify pipe */
    CQ  new_conn_queue;         /* queue of new connections to handle */
} LIBEVENT_THREAD;

static LIBEVENT_THREAD *threads;

/*
 * Number of threads that have finished setting themselves up.
 */
static int init_count = 0;
static pthread_mutex_t init_lock;
static pthread_cond_t init_cond;


static void thread_libevent_process(int fd, short which, void *arg);

/*
 * Initializes a connection queue.
 */
static void cq_init(CQ *cq) {
    pthread_mutex_init(&cq->lock, NULL);
    pthread_cond_init(&cq->cond, NULL);
    cq->head = NULL;
    cq->tail = NULL;
    cq->count = 0;
}

/*
 * Looks for an item on a connection queue, but doesn't block if there isn't
 * one.
 * Returns the item, or NULL if no item is available
 */
static CQ_ITEM *cq_peek(CQ *cq) {
    CQ_ITEM *item;

    pthread_mutex_lock(&cq->lock);
    item = cq->head;
    if (NULL != item) {
        cq->head = item->next;
        if (NULL == cq->head) {
            cq->tail = NULL;
        }
        assert(cq->count > 0);
        cq->count--;
    }
    pthread_mutex_unlock(&cq->lock);

    return item;
}

/*
 * Adds an item to a connection queue.
 */
static void cq_push(CQ *cq, CQ_ITEM *item) {
    item->next = NULL;

    pthread_mutex_lock(&cq->lock);
    if (NULL == cq->tail)
        cq->head = item;
    else
        cq->tail->next = item;
    cq->tail = item;
    cq->count++;
    pthread_cond_signal(&cq->cond);
    pthread_mutex_unlock(&cq->lock);
}


/*
 * Returns a fresh connection queue item.
 */
static CQ_ITEM *cqi_new() {
    CQ_ITEM *item = NULL;
    pthread_mutex_lock(&cqi_freelist_lock);
    if (cqi_freelist) {
        item = cqi_freelist;
        cqi_freelist = item->next;
    }
    pthread_mutex_unlock(&cqi_freelist_lock);

    if (NULL == item) {
        int i;

        /* Allocate a bunch of items at once to reduce fragmentation */
        item = pool_malloc(sizeof(CQ_ITEM) * ITEMS_PER_ALLOC, CQ_POOL);
        if (NULL == item)
            return NULL;

        /*
         * Link together all the new items except the first one
         * (which we'll return to the caller) for placement on
         * the freelist.
         */
        for (i = 2; i < ITEMS_PER_ALLOC; i++)
            item[i - 1].next = &item[i];

        pthread_mutex_lock(&cqi_freelist_lock);
        item[ITEMS_PER_ALLOC - 1].next = cqi_freelist;
        cqi_freelist = &item[1];
        pthread_mutex_unlock(&cqi_freelist_lock);
    }

    return item;
}


/*
 * Frees a connection queue item (adds it to the freelist.)
 */
static void cqi_free(CQ_ITEM *item) {
    pthread_mutex_lock(&cqi_freelist_lock);
    item->next = cqi_freelist;
    cqi_freelist = item;
    pthread_mutex_unlock(&cqi_freelist_lock);
}


/*
 * Creates a worker thread.
 */
static void create_worker(unsigned worker_num, void *(*func)(void *), void *arg) {
    pthread_t       thread;
    pthread_attr_t  attr;
    int             ret;

    pthread_attr_init(&attr);

    if ((ret = pthread_create(&thread, &attr, func, arg)) != 0) {
        fprintf(stderr, "Can't create thread: %s\n",
                strerror(ret));
        exit(1);
    }

    assign_thread_id_to_conn_buffer_group(worker_num - 1 /* worker num is
                                                            1-based, but since
                                                            the main thread does
                                                            not need connection
                                                            buffers, the
                                                            connection buffer
                                                            groups are 0-based. */,
                                          thread);
}


/*
 * Pulls a conn structure from the freelist, if one is available.
 */
conn* mt_conn_from_freelist() {
    conn* c;

    pthread_mutex_lock(&conn_lock);
    c = do_conn_from_freelist();
    pthread_mutex_unlock(&conn_lock);

    return c;
}


/*
 * Adds a conn structure to the freelist.
 *
 * Returns 0 on success, 1 if the structure couldn't be added.
 */
bool mt_conn_add_to_freelist(conn* c) {
    bool result;

    pthread_mutex_lock(&conn_lock);
    result = do_conn_add_to_freelist(c);
    pthread_mutex_unlock(&conn_lock);

    return result;
}

/****************************** LIBEVENT THREADS *****************************/

/*
 * Set up a thread's information.
 */
static void setup_thread(LIBEVENT_THREAD *me) {
    if (! me->base) {
        me->base = event_init();
        if (! me->base) {
            fprintf(stderr, "Can't allocate event base\n");
            exit(1);
        }
    }

    /* Listen for notifications from other threads */
    event_set(&me->notify_event, me->notify_receive_fd,
              EV_READ | EV_PERSIST, thread_libevent_process, me);
    event_base_set(me->base, &me->notify_event);

    if (event_add(&me->notify_event, 0) == -1) {
        fprintf(stderr, "Can't monitor libevent notify pipe\n");
        exit(1);
    }

    cq_init(&me->new_conn_queue);
    me->timer_initialized = false;
}

/*
 * Worker thread: main event loop
 */
static void *worker_libevent(void *arg) {
    LIBEVENT_THREAD *me = arg;

    /* Any per-thread setup can happen here; thread_init() will block until
     * all threads have finished initializing.
     */

    pthread_mutex_lock(&init_lock);
    init_count++;
    pthread_cond_signal(&init_cond);
    pthread_mutex_unlock(&init_lock);
    STATS_SET_TLS(me - threads); /* set thread specific stats structure */
    clock_handler(0, 0, me);

    return (void*) (intptr_t) event_base_loop(me->base, 0);
}


/*
 * Processes an incoming "handle a new connection" item. This is called when
 * input arrives on the libevent wakeup pipe.
 */
static void thread_libevent_process(int fd, short which, void *arg) {
    LIBEVENT_THREAD *me = arg;
    CQ_ITEM *item;
    char buf[1];

    if (read(fd, buf, 1) != 1)
        if (settings.verbose > 0)
            fprintf(stderr, "Can't read from libevent pipe\n");

    item = cq_peek(&me->new_conn_queue);

    if (NULL != item) {
        conn* c = conn_new(item->sfd, item->init_state, item->event_flags,
                           item->cbg, item->is_udp,
                           item->is_binary, &item->addr, item->addrlen,
                           me->base);
        if (c == NULL) {
            if (item->is_udp) {
                fprintf(stderr, "Can't listen for events on UDP socket\n");
                exit(1);
            } else {
                if (settings.verbose > 0) {
                    fprintf(stderr, "Can't listen for events on fd %d\n",
                        item->sfd);
                }
                close(item->sfd);
            }
        }
        cqi_free(item);
    }
}

/* Which thread we assigned a connection to most recently. */
static int last_thread = 0;

/*
 * Dispatches a new connection to another thread. This is only ever called
 * from the main thread, either during initialization (for UDP) or because
 * of an incoming connection.
 */
void dispatch_conn_new(int sfd, int init_state, int event_flags,
                       conn_buffer_group_t* cbg, const bool is_udp, const bool is_binary,
                       const struct sockaddr* const addr, socklen_t addrlen) {
    CQ_ITEM *item = cqi_new();
    /* Count threads from 1..N to skip the dispatch thread.*/
    int tix = (last_thread % (settings.num_threads - 1)) + 1;
    LIBEVENT_THREAD *thread = threads+tix;

    assert(tix != 0); /* Never dispatch to thread 0 */
    last_thread = tix;

    item->sfd = sfd;
    item->init_state = init_state;
    item->event_flags = event_flags;
    if (cbg) {
        item->cbg = cbg;
    } else {
        item->cbg = get_conn_buffer_group(tix - 1);
    }
    item->is_udp = is_udp;
    item->is_binary = is_binary;
    memcpy(&item->addr, addr, addrlen);
    item->addrlen = addrlen;

    cq_push(&thread->new_conn_queue, item);

    if (write(thread->notify_send_fd, "", 1) != 1) {
        perror("Writing to thread notify pipe");
    }
}

/*
 * Returns true if this is the thread that listens for new TCP connections.
 */
int mt_is_listen_thread() {
    return pthread_self() == threads[0].thread_id;
}

void mt_clock_handler(int fd, short which, void *arg)
{
    struct timeval t = {.tv_sec = 1, .tv_usec = 0};
    LIBEVENT_THREAD *me = arg;

    if (me == NULL) { /* thread 0 */
        me = &threads[0];
    }

    if (me->timer_initialized) {
        evtimer_del(&me->timer_event);
    } else {
        me->timer_initialized = true;
    }

    evtimer_set(&me->timer_event, mt_clock_handler, me);
    event_base_set(me->base, &me->timer_event);
    evtimer_add(&me->timer_event, &t);

    /* Only update the current time on the main thread */
    if ((me - threads) == 0) {
        set_current_time();
    }
    update_stats();
}

/********************************* ITEM ACCESS *******************************/

/*
 * Walks through the list of deletes that have been deferred because the items
 * were locked down at the tmie.
 */
void mt_run_deferred_deletes() {
    pthread_mutex_lock(&cache_lock);
    do_run_deferred_deletes();
    pthread_mutex_unlock(&cache_lock);
}

/*
 * Allocates a new item.
 */
item *mt_item_alloc(char *key, size_t nkey, int flags, rel_time_t exptime, int nbytes, const struct in_addr addr) {
    item *it;
    pthread_mutex_lock(&cache_lock);
    it = do_item_alloc(key, nkey, flags, exptime, nbytes, addr);
    pthread_mutex_unlock(&cache_lock);
    return it;
}

/*
 * Returns an item if it hasn't been marked as expired or deleted,
 * lazy-expiring as needed.
 */
item *mt_item_get_notedeleted(const char *key, const size_t nkey, bool *delete_locked) {
    item *it;
    pthread_mutex_lock(&cache_lock);
    it = do_item_get_notedeleted(key, nkey, delete_locked);
    pthread_mutex_unlock(&cache_lock);
    return it;
}

/*
 * Decrements the reference count on an item and adds it to the freelist if
 * needed.
 */
void mt_item_deref(item *item) {
    pthread_mutex_lock(&cache_lock);
    do_item_deref(item);
    pthread_mutex_unlock(&cache_lock);
}

/*
 * Unlinks an item from the LRU and hashtable.
 */
void mt_item_unlink(item *item, long flags, const char* key) {
    pthread_mutex_lock(&cache_lock);
    do_item_unlink(item, flags, key);
    pthread_mutex_unlock(&cache_lock);
}

/*
 * Moves an item to the back of the LRU queue.
 */
void mt_item_update(item *item) {
    pthread_mutex_lock(&cache_lock);
    do_item_update(item);
    pthread_mutex_unlock(&cache_lock);
}

/*
 * Adds an item to the deferred-delete list so it can be reaped later.
 */
int mt_defer_delete(item *item, time_t exptime) {
    int ret;

    pthread_mutex_lock(&cache_lock);
    ret = do_defer_delete(item, exptime);
    pthread_mutex_unlock(&cache_lock);
    return ret;
}

/*
 * Does arithmetic on a numeric item value.
 */
char *mt_add_delta(const char* key, const size_t nkey, const int incr, const unsigned int delta,
                   char *buf, uint32_t *res, const struct in_addr addr) {
    char *ret;

    pthread_mutex_lock(&cache_lock);
    ret = do_add_delta(key, nkey, incr, delta, buf, res, addr);
    pthread_mutex_unlock(&cache_lock);
    return ret;
}

/*
 * Stores an item in the cache (high level, obeys set/add/replace semantics)
 */
int mt_store_item(item *item, int comm, const char* key) {
    int ret;

    pthread_mutex_lock(&cache_lock);
    ret = do_store_item(item, comm, key);
    pthread_mutex_unlock(&cache_lock);
    return ret;
}

/*
 * Flushes expired items after a flush_all call
 */
void mt_item_flush_expired() {
    pthread_mutex_lock(&cache_lock);
    do_item_flush_expired();
    pthread_mutex_unlock(&cache_lock);
}

/*
 * Dumps part of the cache
 */
char *mt_item_cachedump(unsigned int slabs_clsid, unsigned int limit, unsigned int *bytes) {
    char *ret;

    pthread_mutex_lock(&cache_lock);
    ret = do_item_cachedump(slabs_clsid, limit, bytes);
    pthread_mutex_unlock(&cache_lock);
    return ret;
}

#if defined(USE_SLAB_ALLOCATOR)
/*
 * Dumps statistics about slab classes
 */
char *mt_item_stats(int *bytes) {
    char *ret;

    pthread_mutex_lock(&cache_lock);
    ret = do_item_stats(bytes);
    pthread_mutex_unlock(&cache_lock);
    return ret;
}
#endif /* #if defined(USE_SLAB_ALLOCATOR) */

/*
 * Dumps a list of objects of each size in 32-byte increments
 */
char *mt_item_stats_sizes(int *bytes) {
    char *ret;

    pthread_mutex_lock(&cache_lock);
    ret = do_item_stats_sizes(bytes);
    pthread_mutex_unlock(&cache_lock);
    return ret;
}

/*
 * Dumps connect-queue depths for each thread
 */
size_t mt_append_thread_stats(char* const buffer_start,
                              const size_t buffer_size,
                              const size_t buffer_off,
                              const size_t reserved) {
    int ix;
    int off = buffer_off;

    for(ix = 1; ix < settings.num_threads; ix++) {
        off = append_to_buffer(buffer_start, buffer_size, off, reserved,
                               "STAT thread_cq_depth_%d %u\r\n",
                               ix,
                               threads[ix].new_conn_queue.count);
    }
    return off;
}

/****************************** HASHTABLE MODULE *****************************/

int mt_assoc_expire_regex(char *pattern) {
    int ret;

    pthread_mutex_lock(&cache_lock);
    ret = do_assoc_expire_regex(pattern);
    pthread_mutex_unlock(&cache_lock);
    return ret;
}

void mt_assoc_move_next_bucket() {
    pthread_mutex_lock(&cache_lock);
    do_assoc_move_next_bucket();
    pthread_mutex_unlock(&cache_lock);
}

#if defined(USE_SLAB_ALLOCATOR)
/******************************* SLAB ALLOCATOR ******************************/

void *mt_slabs_alloc(size_t size) {
    void *ret;

    pthread_mutex_lock(&slabs_lock);
    ret = do_slabs_alloc(size);
    pthread_mutex_unlock(&slabs_lock);
    return ret;
}

void mt_slabs_free(void *ptr, size_t size) {
    pthread_mutex_lock(&slabs_lock);
    do_slabs_free(ptr, size);
    pthread_mutex_unlock(&slabs_lock);
}

char *mt_slabs_stats(int *buflen) {
    char *ret;

    pthread_mutex_lock(&slabs_lock);
    ret = do_slabs_stats(buflen);
    pthread_mutex_unlock(&slabs_lock);
    return ret;
}

int mt_slabs_reassign(unsigned char srcid, unsigned char dstid) {
    int ret;

    pthread_mutex_lock(&slabs_lock);
    ret = do_slabs_reassign(srcid, dstid);
    pthread_mutex_unlock(&slabs_lock);
    return ret;
}

void mt_slabs_rebalance() {
    pthread_mutex_lock(&slabs_lock);
    do_slabs_rebalance();
    pthread_mutex_unlock(&slabs_lock);
}
#endif /* #if defined(USE_SLAB_ALLOCATOR) */

#if defined(USE_FLAT_ALLOCATOR)
/******************************* FLAT ALLOCATOR ******************************/
char* flat_allocator_stats(size_t* result_size) {
    char* ret;

    pthread_mutex_lock(&cache_lock);
    ret = do_flat_allocator_stats(result_size);
    pthread_mutex_unlock(&cache_lock);
    return ret;
}
#endif /* #if defined(USE_FLAT_ALLOCATOR) */

/******************************* GLOBAL STATS ******************************/

static struct {
    stats_t *stats;
    size_t stats_count;
    pthread_key_t tlsKey;
} l;

void mt_stats_init(int threads) {
    int ix;

    pthread_key_create(&l.tlsKey, NULL);
    l.stats = calloc(threads, sizeof(stats_t));
    l.stats_count = threads;

    for (ix = 0; ix < threads; ix++) {
      stats_t *stats = &l.stats[ix];
      pthread_mutex_init(&stats->lock, NULL);
    }
    stats_prefix_init();
    stats_buckets_init();
    stats_cost_benefit_init();
}

void mt_stats_lock(stats_t *stats) {
    assert(stats->threadid == pthread_self());
    pthread_mutex_lock(&stats->lock);
}

void mt_stats_unlock(stats_t *stats) {
    assert(stats->threadid == pthread_self());
    pthread_mutex_unlock(&stats->lock);
}

void mt_global_stats_lock() {
    pthread_mutex_lock(&gstats_lock);
}

void mt_global_stats_unlock() {
    pthread_mutex_unlock(&gstats_lock);
}

stats_t *mt_stats_get_tls(void) {
    stats_t *stats;
   
    stats = (stats_t *)pthread_getspecific(l.tlsKey);
    assert(stats != NULL);
    assert(stats->threadid == pthread_self());
    return stats;
}

void mt_stats_set_tls(int ix) {
    int rc;

    rc = pthread_setspecific(l.tlsKey, &l.stats[ix]);
    l.stats[ix].threadid = pthread_self();
    assert(rc == 0);
}

void mt_stats_reset(void) {
    stats_t *stats;
    int ix;

    for (ix = 0; ix < l.stats_count; ix++) {
        stats = &l.stats[ix];
        STATS_LOCK(stats);
        stats->total_items = stats->total_conns = 0;
        stats->get_cmds = stats->set_cmds = stats->get_hits = stats->get_misses = stats->evictions = 0;
        stats->arith_cmds = stats->arith_hits = 0;
        stats->bytes_read = stats->bytes_written = 0;
        STATS_UNLOCK(stats);
    }
    stats_prefix_clear();
}

void mt_stats_aggregate(stats_t *accum) {
    stats_t *stats;
    int ix;

#define _AGGREGATE(x)    (accum->x += stats->x)

    memset(accum, 0, sizeof(*accum));
    for (ix = 0; ix < l.stats_count; ix++) {
        stats = &l.stats[ix];
        STATS_LOCK(stats);
        _AGGREGATE(curr_items);
        _AGGREGATE(total_items);
        _AGGREGATE(item_storage_allocated);
        _AGGREGATE(item_total_size);
        _AGGREGATE(curr_conns);
        _AGGREGATE(total_conns);
        _AGGREGATE(conn_structs);
        _AGGREGATE(get_cmds);
        _AGGREGATE(set_cmds);
        _AGGREGATE(get_hits);
        _AGGREGATE(get_misses);
        _AGGREGATE(arith_cmds);
        _AGGREGATE(arith_hits);
        _AGGREGATE(evictions);
        _AGGREGATE(bytes_read);
        _AGGREGATE(bytes_written);
        _AGGREGATE(get_bytes);
        _AGGREGATE(byte_seconds);
#define MEMORY_POOL(pool_enum, pool_counter, pool_string) \
            _AGGREGATE(pool_counter);
#include "memory_pool_classes.h"
#if defined(MEMORY_POOL_CHECKS)
#if defined(MEMORY_POOL_ERROR_BREAKDOWN)
#define MEMORY_POOL(pool_enum, pool_counter, pool_string) \
            _AGGREGATE(mp_bytecount_errors_realloc_split.pool_counter);
#include "memory_pool_classes.h"
#define MEMORY_POOL(pool_enum, pool_counter, pool_string) \
            _AGGREGATE(mp_bytecount_errors_free_split.pool_counter);
#include "memory_pool_classes.h"
#endif /* #if defined(MEMORY_POOL_ERROR_BREAKDOWN) */
        _AGGREGATE(mp_blk_errors);
        _AGGREGATE(mp_bytecount_errors);
        _AGGREGATE(mp_pool_errors);
#endif /* #if defined(MEMORY_POOL_CHECKS) */
        STATS_UNLOCK(stats);
    }
#undef _AGGREGATE
}

/*
 * Initializes the thread subsystem, creating various worker threads.
 *
 * nthreads  Number of event handler threads to spawn
 * main_base Event base for main thread
 */
void thread_init(int nthreads, struct event_base *main_base) {
    int         i;

    pthread_mutexattr_init(&cache_attr);
#ifdef PTHREAD_ADAPTIVE_MUTEX_INITIALIZER_NP
    pthread_mutexattr_settype(&cache_attr, PTHREAD_MUTEX_ADAPTIVE_NP);
#endif
    pthread_mutex_init(&cache_lock, &cache_attr);
    pthread_mutex_init(&conn_lock, NULL);
#if defined(USE_SLAB_ALLOCATOR)
    pthread_mutex_init(&slabs_lock, NULL);
#endif /* #if defined(USE_SLAB_ALLOCATOR) */
    pthread_mutex_init(&gstats_lock, NULL);
    pthread_mutex_init(&conn_buffer_lock, NULL);

    pthread_mutex_init(&init_lock, NULL);
    pthread_cond_init(&init_cond, NULL);

    pthread_mutex_init(&cqi_freelist_lock, NULL);
    cqi_freelist = NULL;

    threads = calloc(nthreads, sizeof(LIBEVENT_THREAD));
    if (! threads) {
        perror("Can't allocate thread descriptors");
        exit(1);
    }

    threads[0].base = main_base;
    threads[0].thread_id = pthread_self();

    for (i = 0; i < nthreads; i++) {
        int fds[2];
        if (pipe(fds)) {
            perror("Can't create notify pipe");
            exit(1);
        }

        threads[i].notify_receive_fd = fds[0];
        threads[i].notify_send_fd = fds[1];

        setup_thread(&threads[i]);
    }

    /* Create threads after we've done all the libevent setup. */
    for (i = 1; i < nthreads; i++) {
        create_worker(i, worker_libevent, &threads[i]);
    }

    /* Wait for all the threads to set themselves up before returning. */
    pthread_mutex_lock(&init_lock);
    init_count++; /* main thread */
    while (init_count < nthreads) {
        pthread_cond_wait(&init_cond, &init_lock);
    }
    pthread_mutex_unlock(&init_lock);
}
