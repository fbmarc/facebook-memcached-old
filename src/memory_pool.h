/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#if !defined(_memory_pool_h_)
#define _memory_pool_h_

#include <stdlib.h>

typedef enum memory_pool_e {
#define MEMORY_POOL(pool_enum, pool_counter, pool_string) pool_enum,
#include "memory_pool_classes.h"
} memory_pool_t;


typedef struct pool_prefix_s pool_prefix_t;
struct pool_prefix_s {
    void* ptr;
    size_t pool_length;
    memory_pool_t pool;
    uint32_t signature;
};

#define MEMORY_POOL_SIGNATURE 0xdeadcafe
// #define MEMORY_POOL_CHECKS
// #define MEMORY_POOL_ERROR_BREAKDOWN

#define pool_malloc(bytes, pool)  pool_malloc_locking(true, bytes, pool)
#define pool_calloc(nmemb, size, pool)  pool_calloc_locking(true, nmemb, size, pool)
#define pool_free(ptr, bytes, pool)  pool_free_locking(true, ptr, bytes, pool)
#define pool_realloc(ptr, bytes, previous, pool)  pool_realloc_locking(true, ptr, bytes, previous, pool)

#if defined(NDEBUG)
#define pool_assert(counter, assertion)         \
    if (! (assertion)) {                        \
        stats.counter ++;                       \
    }
#else
#define pool_assert(counter, assertion) assert(assertion)
#endif /* #if defined(NDEBUG) */


static inline void* pool_malloc_locking(bool do_lock, size_t bytes, memory_pool_t pool) {
#if defined(MEMORY_POOL_CHECKS)
    void* alloc = malloc(bytes + sizeof(pool_prefix_t));
    pool_prefix_t* prefix = alloc;
    void* ret = alloc + sizeof(pool_prefix_t);
    prefix->ptr = ret;
    prefix->pool_length = bytes;
    prefix->pool = pool;
    prefix->signature = MEMORY_POOL_SIGNATURE;
#else /* #if defined(MEMORY_POOL_CHECKS) */
    void* ret = malloc(bytes);
#endif /* #if defined(MEMORY_POOL_CHECKS) */

    if (ret != NULL) {
        switch (pool) {
#define MEMORY_POOL(pool_enum, pool_counter, pool_string) \
            case pool_enum:                               \
                if (do_lock)                              \
                    STATS_LOCK();                         \
                stats.pool_counter += bytes;              \
                if (do_lock)                              \
                    STATS_UNLOCK();                       \
                break;
#include "memory_pool_classes.h"
        }
    }

    return ret;
}


static inline void* pool_calloc_locking(bool do_lock, size_t nmemb, size_t size, memory_pool_t pool) {
    size_t bytes = nmemb * size;
#if defined(MEMORY_POOL_CHECKS)
    void* alloc = malloc(bytes + sizeof(pool_prefix_t));
    pool_prefix_t* prefix = alloc;
    void* ret = alloc + sizeof(pool_prefix_t);
    prefix->ptr = ret;
    prefix->pool_length = bytes;
    prefix->pool = pool;
    prefix->signature = MEMORY_POOL_SIGNATURE;
    memset(ret, 0, bytes);
#else /* #if defined(MEMORY_POOL_CHECKS) */
    void* ret = calloc(nmemb, size);
#endif /* #if defined(MEMORY_POOL_CHECKS) */

    if (ret != NULL) {
        switch (pool) {
#define MEMORY_POOL(pool_enum, pool_counter, pool_string) \
            case pool_enum:                               \
                if (do_lock)                              \
                    STATS_LOCK();                         \
                stats.pool_counter += bytes;              \
                if (do_lock)                              \
                    STATS_UNLOCK();                       \
                break;
#include "memory_pool_classes.h"
        }
    }

    return ret;
}


static inline void pool_free_locking(bool do_lock, void* ptr, size_t previous, memory_pool_t pool) {
#if defined(MEMORY_POOL_CHECKS)
    pool_prefix_t* prefix = ptr;
    void* alloc;
#if defined(MEMORY_POOL_ERROR_BREAKDOWN)
    size_t saved_pool_length;
#endif /* #if defined(MEMORY_POOL_ERROR_BREAKDOWN) */

    prefix --;
    alloc = prefix;

#if defined(MEMORY_POOL_ERROR_BREAKDOWN)
    saved_pool_length = prefix->pool_length;
#endif /* #if defined(MEMORY_POOL_ERROR_BREAKDOWN) */

    pool_assert(mp_blk_errors, prefix->ptr == ptr);
    pool_assert(mp_bytecount_errors, prefix->pool_length == previous);
    pool_assert(mp_pool_errors, prefix->pool == pool);
    pool_assert(mp_blk_errors, prefix->signature == MEMORY_POOL_SIGNATURE);
    free(alloc);
#else /* #if defined(MEMORY_POOL_CHECKS) */
    free(ptr);
#endif /* #if defined(MEMORY_POOL_CHECKS) */

    switch (pool) {
#if defined(MEMORY_POOL_ERROR_BREAKDOWN)
#define MEMORY_POOL(pool_enum, pool_counter, pool_string)               \
        case pool_enum:                                                 \
            if (do_lock) {                                              \
                STATS_LOCK();                                           \
            }                                                           \
            stats.pool_counter -= previous;                             \
            if (saved_pool_length != previous) {                        \
                stats.mp_bytecount_errors_free_split.pool_counter ++;   \
            }                                                           \
            if (do_lock) {                                              \
                STATS_UNLOCK();                                         \
            }                                                           \
            break;
#else
#define MEMORY_POOL(pool_enum, pool_counter, pool_string)               \
        case pool_enum:                                                 \
            if (do_lock) {                                              \
                STATS_LOCK();                                           \
            }                                                           \
            stats.pool_counter -= previous;                             \
            if (do_lock) {                                              \
                STATS_UNLOCK();                                         \
            }                                                           \
            break;
#endif /* #if defined(MEMORY_POOL_ERROR_BREAKDOWN) */
#include "memory_pool_classes.h"
    }
}


static inline void* pool_realloc_locking(bool do_lock, void* ptr, size_t bytes, size_t previous, memory_pool_t pool) {
#if defined(MEMORY_POOL_CHECKS)
    pool_prefix_t* prefix = ptr;
    void* alloc, * ret;
#if defined(MEMORY_POOL_ERROR_BREAKDOWN)
    size_t saved_pool_length;
#endif /* #if defined(MEMORY_POOL_ERROR_BREAKDOWN) */

    prefix --;
    alloc = prefix;

#if defined(MEMORY_POOL_ERROR_BREAKDOWN)
    saved_pool_length = prefix->pool_length;
#endif /* #if defined(MEMORY_POOL_ERROR_BREAKDOWN) */

    pool_assert(mp_blk_errors, prefix->ptr == ptr);
    pool_assert(mp_bytecount_errors, prefix->pool_length == previous);
    pool_assert(mp_pool_errors, prefix->pool == pool);
    pool_assert(mp_blk_errors, prefix->signature == MEMORY_POOL_SIGNATURE);
    alloc = realloc(alloc, bytes + sizeof(pool_prefix_t));
    if (alloc == NULL) {
        return NULL;
    }
    prefix = alloc;
    ret = alloc + sizeof(pool_prefix_t);
    prefix->ptr = ret;
    prefix->pool_length = bytes;
#else /* #if defined(MEMORY_POOL_CHECKS) */
    void* ret = realloc(ptr, bytes);
#endif /* #if defined(MEMORY_POOL_CHECKS) */

    if (ret != NULL) {
        switch (pool) {
#if defined(MEMORY_POOL_ERROR_BREAKDOWN)
#define MEMORY_POOL(pool_enum, pool_counter, pool_string)               \
            case pool_enum:                                             \
                if (do_lock) {                                          \
                    STATS_LOCK();                                       \
                }                                                       \
                stats.pool_counter -= previous;                         \
                stats.pool_counter += bytes;                            \
                if (saved_pool_length != previous) {                    \
                    stats.mp_bytecount_errors_realloc_split.pool_counter ++; \
                }                                                       \
                if (do_lock) {                                          \
                    STATS_UNLOCK();                                     \
                }                                                       \
            break;
#else
#define MEMORY_POOL(pool_enum, pool_counter, pool_string)               \
            case pool_enum:                                             \
                if (do_lock) {                                          \
                    STATS_LOCK();                                       \
                }                                                       \
                stats.pool_counter -= previous;                         \
                stats.pool_counter += bytes;                            \
                if (do_lock) {                                          \
                    STATS_UNLOCK();                                     \
                }                                                       \
            break;
#endif /* #if defined(MEMORY_POOL_ERROR_BREAKDOWN) */
#include "memory_pool_classes.h"
        }
    }

    return ret;
}

#endif /* #if !defined(_memory_pool_h_) */
