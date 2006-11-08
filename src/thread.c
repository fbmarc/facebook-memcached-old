/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Thread management for memcached.
 *
 *  $Id$
 */
#include "memcached.h"
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <errno.h>

#ifdef HAVE_MALLOC_H
#include <malloc.h>
#endif

#ifdef USE_THREADS

#include <pthread.h>

#define ITEMS_PER_ALLOC 64

/* An item in a work queue. */
typedef struct work_queue_item WQ_ITEM;
struct work_queue_item {
    void    *arg;
    WQ_ITEM *next;
};

/* A work queue. */
typedef struct work_queue WQ;
struct work_queue {
    WQ_ITEM *head;
    WQ_ITEM *tail;
    pthread_mutex_t lock;
    pthread_cond_t  cond;
};

/* Lock for connection freelist */
static pthread_mutex_t conn_lock;

/* Lock for cache operations (item_*, assoc_*) */
static pthread_mutex_t cache_lock;

/* Lock for slab allocator operations */
static pthread_mutex_t slabs_lock;

/* Free list of WQ_ITEM structs */
static WQ_ITEM *wqi_freelist;
static pthread_mutex_t wqi_freelist_lock;

/* Various work queues. */
static WQ state_machine_queue;

/*
 * Each libevent instance has a wakeup pipe, which other threads
 * can use to signal that they've put work on its work queue.
 */
typedef struct {
    pthread_t thread_id;        /* unique ID of this thread */
    struct event_base *base;    /* libevent handle this thread uses */
    struct event notify_event;  /* listen event for notify pipe */
    int notify_receive_fd;      /* receiving end of notify pipe */
    int notify_send_fd;         /* sending end of notify pipe */
    WQ  new_conn_queue;         /* queue of new connections to handle */
} LIBEVENT_THREAD;

static LIBEVENT_THREAD *threads;

/*
 * This gets set when we initialize, which happens after listening sockets
 * are bound.
 */
static int thread_initialized = 0;

/*
 * Initializes a work queue.
 */
static void wq_init(WQ *wq) {
    pthread_mutex_init(&wq->lock, NULL);
    pthread_cond_init(&wq->cond, NULL);
    wq->head = NULL;
    wq->tail = NULL;
}

/*
 * Waits for work on a work queue.
 */
static WQ_ITEM *wq_pop(WQ *wq) {
    WQ_ITEM *item;

    pthread_mutex_lock(&wq->lock);
    while (NULL == wq->head)
        pthread_cond_wait(&wq->cond, &wq->lock);
    item = wq->head;
    wq->head = item->next;
    if (NULL == wq->head)
        wq->tail = NULL;
    pthread_mutex_unlock(&wq->lock);

    return item;
}

/*
 * Looks for an item on a work queue, but doesn't block if there isn't one.
 */
static WQ_ITEM *wq_peek(WQ *wq) {
    WQ_ITEM *item;

    pthread_mutex_lock(&wq->lock);
    item = wq->head;
    if (NULL != item) {
        wq->head = item->next;
        if (NULL == wq->head)
            wq->tail = NULL;
    }
    pthread_mutex_unlock(&wq->lock);

    return item;
}

/*
 * Adds an item to a work queue.
 */
static void wq_push(WQ *wq, WQ_ITEM *item) {
    item->next = NULL;

    pthread_mutex_lock(&wq->lock);
    if (NULL == wq->tail)
        wq->head = item;
    else
        wq->tail->next = item;
    wq->tail = item;
    pthread_cond_signal(&wq->cond);
    pthread_mutex_unlock(&wq->lock);
}

/*
 * Returns a fresh work queue item for a connection.
 */
static WQ_ITEM *wqi_new(void *c) {
    WQ_ITEM *item = NULL;
    pthread_mutex_lock(&wqi_freelist_lock);
    if (wqi_freelist) {
        item = wqi_freelist;
        wqi_freelist = item->next;
    }
    pthread_mutex_unlock(&wqi_freelist_lock);

    if (NULL == item) {
        int i;

        /* Allocate a bunch of items at once to reduce fragmentation */
        item = malloc(sizeof(WQ_ITEM) * ITEMS_PER_ALLOC);
        if (NULL == item)
            return NULL;

        /*
         * Link together all the new items except the first one
         * (which we'll return to the caller) for placement on
         * the freelist.
         */
        for (i = 2; i < ITEMS_PER_ALLOC - 1; i++)
            item[i - 1].next = &item[i];

        pthread_mutex_lock(&wqi_freelist_lock);
        item[ITEMS_PER_ALLOC - 1].next = wqi_freelist;
        wqi_freelist = &item[1];
        pthread_mutex_unlock(&wqi_freelist_lock);
    }

    item->arg = c;
    return item;
}


/*
 * Frees a work queue item (adds it to the freelist.)
 */
static void wqi_free(WQ_ITEM *item) {
    item->arg = NULL;

    pthread_mutex_lock(&wqi_freelist_lock);
    item->next = wqi_freelist;
    wqi_freelist = item;
    pthread_mutex_unlock(&wqi_freelist_lock);
}


/*
 * Creates a worker thread.
 */
static void create_worker(void *(*func)(void *), void *arg) {
    pthread_t       thread;
    pthread_attr_t  attr;
    int             ret;

    pthread_attr_init(&attr);

    if (ret = pthread_create(&thread, &attr, func, arg)) {
        fprintf(stderr, "Can't create thread: %s\n",
                strerror(ret));
        exit(1);
    }
}


/*
 * Pulls a conn structure from the freelist, if one is available.
 */
conn *mt_conn_from_freelist() {
    conn *c;

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
int mt_conn_add_to_freelist(conn *c) {
    int result;

    pthread_mutex_lock(&conn_lock);
    result = do_conn_add_to_freelist(c);
    pthread_mutex_unlock(&conn_lock);

    return result;
}

/****************************** LIBEVENT THREADS *****************************/

/*
 * Worker thread: main event loop
 */
static void *worker_libevent(void *arg) {
    LIBEVENT_THREAD *me = arg;

    event_base_loop(me->base, 0);
}


/*
 * Processes an incoming "add to libevent" work item. This is called when
 * input arrives on the libevent wakeup pipe.
 */
void thread_libevent_process(int fd, short which, void *arg) {
    LIBEVENT_THREAD *me = arg;
    WQ_ITEM *item;
    char buf[1];

    if (read(fd, buf, 1) != 1)
        if (settings.verbose > 0)
            fprintf(stderr, "Can't read from libevent pipe\n");

    if (item = wq_peek(&me->new_conn_queue)) {
        conn *c = item->arg;

        event_base_set(me->base, &c->event);
        if (event_add(&c->event, 0) < 0) {
            fprintf(stderr, "Can't monitor connection\n");
            if (conn_add_to_freelist(c))
                conn_free(c);
        }

        wqi_free(item);
    }
}

/*
 * Dispatches an "add" request to a libevent thread.
 */
int dispatch_event_add(int thread, conn *c) {
    WQ_ITEM *item = wqi_new(c);

    if (! thread_initialized)
        return event_add(&c->event, 0);

    item->arg = c;
    wq_push(&threads[thread].new_conn_queue, item);
    return write(threads[thread].notify_send_fd, "", 1) == 1;
}

/*
 * Returns true if this is the thread that listens for new TCP connections.
 */
int mt_is_listen_thread() {
    return pthread_self() == threads[0].thread_id;
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
item *mt_item_alloc(char *key, size_t nkey, int flags, rel_time_t exptime, int nbytes) {
    item *it;
    pthread_mutex_lock(&cache_lock);
    it = do_item_alloc(key, nkey, flags, exptime, nbytes);
    pthread_mutex_unlock(&cache_lock);
    return it;
}

/*
 * Returns an item if it hasn't been marked as expired or deleted,
 * lazy-expiring as needed.
 */
item *mt_item_get_notedeleted(char *key, size_t nkey, int *delete_locked) {
    item *it;
    pthread_mutex_lock(&cache_lock);
    it = do_item_get_notedeleted(key, nkey, delete_locked);
    pthread_mutex_unlock(&cache_lock);
    return it;
}

/*
 * Returns an item whether or not it's been marked as expired or deleted.
 */
item *mt_item_get_nocheck(char *key, size_t nkey) {
    item *it;

    pthread_mutex_lock(&cache_lock);
    it = assoc_find(key, nkey);
    it->refcount++;
    pthread_mutex_unlock(&cache_lock);
    return it;
}

/*
 * Links an item into the LRU and hashtable.
 */
int mt_item_link(item *item) {
    int ret;

    pthread_mutex_lock(&cache_lock);
    ret = do_item_link(item);
    pthread_mutex_unlock(&cache_lock);
    return ret;
}

/*
 * Decrements the reference count on an item and adds it to the freelist if
 * needed.
 */
void mt_item_remove(item *item) {
    pthread_mutex_lock(&cache_lock);
    do_item_remove(item);
    pthread_mutex_unlock(&cache_lock);
}

/*
 * Replaces one item with another in the hashtable.
 */
int mt_item_replace(item *old, item *new) {
    int ret;

    pthread_mutex_lock(&cache_lock);
    ret = do_item_replace(old, new);
    pthread_mutex_unlock(&cache_lock);
    return ret;
}

/*
 * Unlinks an item from the LRU and hashtable.
 */
void mt_item_unlink(item *item) {
    pthread_mutex_lock(&cache_lock);
    do_item_unlink(item);
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
char *mt_defer_delete(item *item, time_t exptime) {
    char *ret;

    pthread_mutex_lock(&cache_lock);
    ret = do_defer_delete(item, exptime);
    pthread_mutex_unlock(&cache_lock);
    return ret;
}

/*
 * Does arithmetic on a numeric item value.
 */
char *mt_add_delta(item *item, int incr, unsigned int delta, char *buf) {
    char *ret;

    pthread_mutex_lock(&cache_lock);
    ret = do_add_delta(item, incr, delta, buf);
    pthread_mutex_unlock(&cache_lock);
    return ret;
}

/*
 * Stores an item in the cache (high level, obeys set/add/replace semantics)
 */
int mt_store_item(item *item, int comm) {
    int ret;

    pthread_mutex_lock(&cache_lock);
    ret = do_store_item(item, comm);
    pthread_mutex_unlock(&cache_lock);
    return ret;
}

/****************************** HASHTABLE MODULE *****************************/

void mt_assoc_move_next_bucket() {
    pthread_mutex_lock(&cache_lock);
    do_assoc_move_next_bucket();
    pthread_mutex_unlock(&cache_lock);
}

int mt_assoc_expire_regex(char *pattern) {
    int ret;

    pthread_mutex_lock(&cache_lock);
    ret = do_assoc_expire_regex(pattern);
    pthread_mutex_unlock(&cache_lock);
    return ret;
}

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

#ifdef ALLOW_SLABS_REASSIGN
int mt_slabs_reassign(unsigned char srcid, unsigned char dstid) {
    int ret;

    pthread_mutex_lock(&slabs_lock);
    ret = do_slabs_reassign(srcid, dstid);
    pthread_mutex_unlock(&slabs_lock);
    return ret;
}
#endif


/*
 * Initializes the thread subsystem, creating various worker threads.
 *
 * nthreads Number of event handler threads to spawn
 * bases    Array of event_base* to use for worker threads
 */
void thread_init(int nthreads, struct event_base **bases) {
    int         i;
    pthread_t   *thread;

    pthread_mutex_init(&cache_lock, NULL);
    pthread_mutex_init(&conn_lock, NULL);
    pthread_mutex_init(&slabs_lock, NULL);

    pthread_mutex_init(&wqi_freelist_lock, NULL);
    wqi_freelist = NULL;

    wq_init(&state_machine_queue);

    threads = malloc(sizeof(LIBEVENT_THREAD) * nthreads);
    if (! threads) {
        perror("Can't allocate thread descriptors");
        exit(1);
    }

    for (i = 0; i < nthreads; i++) {
        int fds[2];
        if (pipe(fds)) {
            perror("Can't create notify pipe");
            exit(1);
        }

        threads[i].notify_receive_fd = fds[0];
        threads[i].notify_send_fd = fds[1];
        threads[i].base = bases[i];

        /* Listen for notifications from other threads */
        event_set(&threads[i].notify_event, fds[0],
                  EV_READ | EV_PERSIST, thread_libevent_process,
                  &threads[i]);
        event_base_set(threads[i].base, &threads[i].notify_event);

        if (event_add(&threads[i].notify_event, 0) == -1) {
            fprintf(stderr, "Can't monitor libevent notify pipe\n");
            exit(1);
        }

        wq_init(&threads[i].new_conn_queue);

        /* The main thread will service base #0. */
        if (i > 0) {
            create_worker(worker_libevent, &threads[i]);
        }
        else {
            threads[i].thread_id = pthread_self();
        }
    }

    thread_initialized = 1;
}

#endif
