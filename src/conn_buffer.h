/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "generic.h"

#if !defined(_conn_buffer_h_)
#define _conn_buffer_h_

#if defined(CONN_BUFFER_TESTS)
#define STATIC
#if defined(CONN_BUFFER_MODULE)
#define STATIC_DECL(decl) decl
#else
#define STATIC_DECL(decl) extern decl
#endif /* #if defined(CONN_BUFFER_MODULE) */

#else
#define STATIC static
#if defined(CONN_BUFFER_MODULE)
#define STATIC_DECL(decl) static decl
#else
#define STATIC_DECL(decl)
#endif /* #if defined(CONN_BUFFER_MODULE) */
#endif /* #if defined(CONN_BUFFER_TESTS) */

#define CONN_BUFFER_INITIAL_BUFFER_COUNT_DEFAULT     (8)
#define CONN_BUFFER_RSIZE_LIMIT_DEFAULT              (128 * 1024)
#define CONN_BUFFER_TOTAL_RSIZE_RANGE_BOTTOM_DEFAULT (8 * 1024 * 1024)
#define CONN_BUFFER_TOTAL_RSIZE_RANGE_TOP_DEFAULT    (16 * 1024 * 1024)

#define CONN_BUFFER_SIZE (16 * 1024 * 1024)
#define CONN_BUFFER_SIGNATURE  (0xbeadbeef)

#define CONN_BUFFER_HEADER_CONTENTS             \
    uint32_t signature;                         \
    uint32_t prev_rusage;                       \
    uint32_t max_rusage;                        \
    uint8_t unused[3];                          \
    unsigned char :5;                           \
    unsigned char rusage_updated:1;             \
    unsigned char in_freelist:1;                \
    unsigned char used:1;

#define CONN_BUFFER_HEADER_SZ sizeof(struct { CONN_BUFFER_HEADER_CONTENTS })

#define CONN_BUFFER_DATA_SZ   (CONN_BUFFER_SIZE - CONN_BUFFER_HEADER_SZ)
typedef struct conn_buffer_s conn_buffer_t;
struct conn_buffer_s {
    CONN_BUFFER_HEADER_CONTENTS;
    unsigned char data[CONN_BUFFER_DATA_SZ];
};


typedef struct conn_buffer_status_s conn_buffer_status_t;
struct conn_buffer_status_s {
    conn_buffer_t** free_buffers;
    size_t num_free_buffers;
    size_t free_buffer_list_capacity;

    size_t total_rsize;
    size_t total_rsize_in_freelist;

    bool reclamation_in_progress;
    bool initialized;

    struct {
        size_t initial_buffer_count;    /* initial buffers set up */
        size_t buffer_rsize_limit;      /* if the reported usage of a block is
                                         * greater or equal to this limit, the
                                         * block is immediately returned to the
                                         * OS.  otherwise, the block is
                                         * recycled. */
        size_t total_rsize_range_top;   /* the total reported usage of the
                                         * conn_buffer system is not to exceed
                                         * this limit. */
        size_t total_rsize_range_bottom;/* when the total_rsize has hit
                                         * total_rsize_range_top, buffers are
                                         * returned to the OS until we reach
                                         * this value *or* there's nothing else
                                         * on the freelist to return. */
        size_t page_size;               /* page size on the OS. */
    } settings;

    struct {
        uint64_t allocs;
        uint64_t frees;
        uint64_t destroys;
        uint64_t reclamations_started;
        uint64_t allocs_failed;
    } stats;
};


DECL_MT_FUNC(void*, alloc_conn_buffer,       (size_t max_rusage_hint));
DECL_MT_FUNC(void,  free_conn_buffer,        (void* ptr, ssize_t max_rusage));
DECL_MT_FUNC(void,  conn_buffer_reclamation, (void));
DECL_MT_FUNC(char*, conn_buffer_stats,       (size_t* result_size));

extern void report_max_rusage(void* ptr, size_t max_rusage);

extern void conn_buffer_init(size_t initial_buffer_count,
                             size_t buffer_rsize_limit,
                             size_t total_rsize_range_bottom,
                             size_t total_rsize_range_top);

STATIC_DECL(int cb_freelist_check(void));
STATIC_DECL(conn_buffer_status_t cbs);

#if !defined(CONN_BUFFER_MODULE)
#undef STATIC
#undef STATIC_DECL
#else
#undef CONN_BUFFER_MODULE
#endif /* #if !defined(CONN_BUFFER_MODULE) */

#endif /* #if !defined(_conn_buffer_h_) */
