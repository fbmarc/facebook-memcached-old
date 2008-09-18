/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/**
 * overview
 * --------
 *
 * objects are memcache key-value pairs.  chunks are units of memory used to
 * store the objects.  objects are classified as either a large object or a
 * small object.  a large object is stored using one or more large chunks.  a
 * small object is stored using one or more small chunks.  there is one
 * exception to this rule -- if the key is so large that it spans multiple small
 * chunks, it will be stored in a large chunk.  each object consists of a title
 * chunk, optionally followed by body chunks.
 *
 *
 * allocation strategy
 * -------------------
 *
 * when allocating for a large object:
 *
 * 1) check large chunk free list.  if there are free chunks, use them.
 *
 * 2) ask the master storage controller for more memory.  if that succeeds, the
 *    free lists will be updated.
 *
 * 3) if the master storage controller has no more memory:
 *    a) if access-time(small) OLDER access-time(large), invalidate small.
 *       find remaining objects in the same slab as small.  retain objects
 *       that are accessed more recently than access-time(small-queue).
 *    b) if access-time(small) YOUNGER access-time(large), dequeue from large
 *       LRU queue.
 *
 * when allocating for a small object:
 *
 * 1) check small chunk free list.  if there are free chunks, use them.
 *
 * 2) check large chunk free list.  if there are free chunks, break it up into
 *    small chunks and update the small chunk free lists.
 *
 * 3) ask the master storage controller for more memory.  if that succeeds, the
 *    free lists will be updated.
 *
 * 4) if the master storage controller has no more memory:
 *    a) if access-time(small) OLDER access-time(large), dequeue from small
 *       LRU queue.
 *    b) if access-time(small) YOUNGER access-time(large), break up large
 *       object into small objects.
 *
 *
 * structures
 * ----------
 *
 * each large chunk consists of:
 *    - a flag indicating whether the chunk is in use.
 *    - a flag indicating if the chunk is used as a whole or as multiple
 *      small chunks.
 *    - a flag indicating whether the chunk is a title chunk or a body chunk.
 *
 * if the large chunk is split into multiple small chunks, it contains an array
 * with fields for each component small chunk:
 *    - a flag indicating whether the chunk is in use.
 *    - a flag indicating whether the chunk is a title chunk or a body chunk.
 *
 *
 * title chunks also contain:
 *     next                    # LRU next
 *     prev                    # LRU prev
 *     h_next                  # hash next
 *     chunk_next              # chunk next
 *     rel_time_t time
 *     rel_time_t exptime
 *     int nbytes
 *     unsigned short refcount
 *     uint8_t it_flags
 *     uint8_t nkey            # key length.
 *     data
 *
 * body chunks contain:
 *     pointer to title
 *     data
 */

#if !defined(_flat_storage_h_)
#define _flat_storage_h_

#include "generic.h"

#include <assert.h>
#include <string.h>
#include <stdint.h>

#if defined(__GNUC__)
#define PACKED __attribute__((packed))
#endif

#if defined(FLAT_STORAGE_TESTS)
#define FA_STATIC
#if defined(FLAT_STORAGE_MODULE)
#define FA_STATIC_DECL(decl) decl
#else
#define FA_STATIC_DECL(decl) extern decl
#endif /* #if defined(FLAT_STORAGE_MODULE) */

#else
#define FA_STATIC static
#if defined(FLAT_STORAGE_MODULE)
#define FA_STATIC_DECL(decl) static decl
#else
#define FA_STATIC_DECL(decl)
#endif /* #if defined(FLAT_STORAGE_MODULE) */
#endif /* #if defined(FLAT_STORAGE_TESTS) */

/**
 * constants
 */

typedef enum large_chunk_flags_e {
    LARGE_CHUNK_INITIALIZED = 0x1,      /* if set, that means the chunk has been
                                         * initialized. */
    LARGE_CHUNK_USED        = 0x2,      /* if set, chunk is used. */
    LARGE_CHUNK_BROKEN      = 0x4,      /* if set, chunk is used as a small
                                         * chunks. */
    LARGE_CHUNK_TITLE       = 0x8,      /* if set, chunk is a title.  it is a
                                         * contradiction for both
                                         * LARGE_CHUNK_BROKEN and
                                         * LARGE_CHUNK_TITLE to be set. */
    LARGE_CHUNK_FREE        = 0x10,     /* if set, chunk is free. */
} large_chunk_flags_t;


typedef enum small_chunk_flags_e {
    SMALL_CHUNK_INITIALIZED = 0x1,      /* if set, that means the chunk has been
                                         * initialized. */
    SMALL_CHUNK_USED        = 0x2,      /* if set, chunk is used. */
    SMALL_CHUNK_TITLE       = 0x8,      /* if set, chunk is a title. */
    SMALL_CHUNK_FREE        = 0x10,     /* if set, chunk is free. */
    SMALL_CHUNK_COALESCE_PENDING = 0x20, /* if set, chunk is free but pending a
                                          * coalesce.  it should *not* be in
                                          * the free list. */
} small_chunk_flags_t;


typedef enum it_flags_e {
    ITEM_VALID   = 0x1,
    ITEM_LINKED  = 0x2,                 /* linked into the LRU. */
    ITEM_DELETED = 0x4,                 /* deferred delete. */
    ITEM_HAS_IP_ADDRESS = 0x10,
    ITEM_HAS_TIMESTAMP = 0x20,
} it_flags_t;


typedef enum chunk_type_e {
    SMALL_CHUNK,
    LARGE_CHUNK,
} chunk_type_t;


#define LARGE_CHUNK_SZ       1024       /* large chunk size */
#define SMALL_CHUNK_SZ       124        /* small chunk size */

#define FLAT_STORAGE_INCREMENT_DELTA (LARGE_CHUNK_SZ * 1024) /* initialize 2k
                                                              * chunks at a time. */

/** instead of using raw pointers, we use chunk pointers.  we address things
 * intervals of CHUNK_ADDRESSING_SZ.  it is possible for SMALL_CHUNK_SZ to be
 * smaller than the addressing interval, as long as we can still uniquely
 * identify each small chunk.  this means that:
 *   floor(LARGE_CHUNK_SZ / SMALL_CHUNK_SZ) <=
 *         floor(LARGE_CHUNK_SZ / CHUNK_ADDRESSING_SZ)
 *
 * we will check for this condition in an assert in items_init(..).
 */
#define CHUNK_ADDRESSING_SZ  128
#define SMALL_CHUNKS_PER_LARGE_CHUNK ((LARGE_CHUNK_SZ - LARGE_CHUNK_TAIL_SZ) / (SMALL_CHUNK_SZ))

#define MIN_LARGE_CHUNK_CAPACITY ((LARGE_TITLE_CHUNK_DATA_SZ <= LARGE_BODY_CHUNK_DATA_SZ) ? \
                                  LARGE_TITLE_CHUNK_DATA_SZ : LARGE_BODY_CHUNK_DATA_SZ) /* this is the largeest number of data
                                                                                         * bytes a large chunk can hold. */

#define MIN_SMALL_CHUNK_CAPACITY ((SMALL_TITLE_CHUNK_DATA_SZ <= SMALL_BODY_CHUNK_DATA_SZ) ? \
                                  SMALL_TITLE_CHUNK_DATA_SZ : SMALL_BODY_CHUNK_DATA_SZ) /* this is the smallest number of data
                                                                                         * bytes a small chunk can hold. */

#define LRU_SEARCH_DEPTH   50           /* number of items we'll check in the
                                         * LRU to find items to evict. */

/**
 * data types and structures
 */

typedef uint32_t chunkptr_t;
typedef chunkptr_t item_ptr_t;
#define NULL_CHUNKPTR ((chunkptr_t) (0))
#define NULL_ITEM_PTR ((item_ptr_t) (0))

/**
 * forward declarations
 */
typedef union chunk_u chunk_t;
typedef union item_u item;
typedef struct large_chunk_s large_chunk_t;
typedef struct small_chunk_s small_chunk_t;

#define TITLE_CHUNK_HEADER_CONTENTS                                     \
    item_ptr_t h_next;                      /* hash next */             \
    chunkptr_t next;                        /* LRU next */              \
    chunkptr_t prev;                        /* LRU prev */              \
    chunkptr_t next_chunk;                  /* next chunk */            \
    rel_time_t time;                        /* most recent access */    \
    rel_time_t exptime;                     /* expire time */           \
    int nbytes;                             /* size of data */          \
    unsigned int flags;                     /* flags */                 \
    unsigned short refcount;                                            \
    uint8_t it_flags;                       /* it flags */              \
    uint8_t nkey;                           /* key length */            \


#define LARGE_BODY_CHUNK_HEADER                 \
    chunkptr_t next_chunk;

#define SMALL_BODY_CHUNK_HEADER                 \
    chunkptr_t prev_chunk;                      \
    chunkptr_t next_chunk;

#define LARGE_CHUNK_TAIL                        \
    uint8_t flags;

#define SMALL_CHUNK_TAIL                        \
    uint8_t flags;

#define LARGE_CHUNK_TAIL_SZ   sizeof(struct { LARGE_CHUNK_TAIL } PACKED)
#define SMALL_CHUNK_TAIL_SZ   sizeof(struct { SMALL_CHUNK_TAIL } PACKED)
#define TITLE_CHUNK_HEADER_SZ sizeof(title_chunk_header_t)
#define LARGE_BODY_CHUNK_HEADER_SZ  sizeof(struct { LARGE_BODY_CHUNK_HEADER } PACKED)
#define SMALL_BODY_CHUNK_HEADER_SZ  sizeof(struct { SMALL_BODY_CHUNK_HEADER } PACKED)

typedef struct title_chunk_header_s title_chunk_header_t;
struct title_chunk_header_s {
    TITLE_CHUNK_HEADER_CONTENTS;
} PACKED;

#define LARGE_TITLE_CHUNK_DATA_SZ (LARGE_CHUNK_SZ - LARGE_CHUNK_TAIL_SZ - TITLE_CHUNK_HEADER_SZ)
typedef struct large_title_chunk_s large_title_chunk_t;
struct large_title_chunk_s {
    TITLE_CHUNK_HEADER_CONTENTS;
    char data[LARGE_TITLE_CHUNK_DATA_SZ];
} PACKED;

#define LARGE_BODY_CHUNK_DATA_SZ (LARGE_CHUNK_SZ - LARGE_CHUNK_TAIL_SZ - LARGE_BODY_CHUNK_HEADER_SZ)
typedef struct large_body_chunk_s large_body_chunk_t;
struct large_body_chunk_s {
    LARGE_BODY_CHUNK_HEADER;
    char data[LARGE_BODY_CHUNK_DATA_SZ];
} PACKED;

typedef struct large_free_chunk_s large_free_chunk_t;
struct large_free_chunk_s {
    /* large chunks do not need prev_next and next_prev pointers because we never remove an element
     * from the middle of the free list. */
    large_chunk_t* next;
    large_chunk_t* prev;
};

#define SMALL_TITLE_CHUNK_DATA_SZ (SMALL_CHUNK_SZ - SMALL_CHUNK_TAIL_SZ - TITLE_CHUNK_HEADER_SZ)
typedef struct small_title_chunk_s small_title_chunk_t;
struct small_title_chunk_s {
    TITLE_CHUNK_HEADER_CONTENTS;
    char data[SMALL_TITLE_CHUNK_DATA_SZ];
} PACKED;

#define SMALL_BODY_CHUNK_DATA_SZ (SMALL_CHUNK_SZ - SMALL_CHUNK_TAIL_SZ - SMALL_BODY_CHUNK_HEADER_SZ)
typedef struct small_body_chunk_s small_body_chunk_t;
struct small_body_chunk_s {
    SMALL_BODY_CHUNK_HEADER;
    char data[SMALL_BODY_CHUNK_DATA_SZ];
} PACKED;

typedef struct small_free_chunk_s small_free_chunk_t;
struct small_free_chunk_s {
    small_chunk_t** prev_next;
    small_chunk_t* next;
};


#define sc_title  __sc.__sc_title
#define sc_body   __sc.__sc_body
#define sc_free   __sc.__sc_free
struct small_chunk_s {
    /* we could be one of 3 things:
     *  1) a small chunk (sc) title.
     *  2) a small chunk (sc) body.
     *  3) a free chunk. */
    union {
        small_title_chunk_t __sc_title;
        small_body_chunk_t  __sc_body;
        small_free_chunk_t __sc_free;
    } PACKED __sc;

    SMALL_CHUNK_TAIL;
} PACKED;


typedef struct large_broken_chunk_s large_broken_chunk_t;
struct large_broken_chunk_s {
    small_chunk_t lbc[SMALL_CHUNKS_PER_LARGE_CHUNK];
    uint8_t small_chunks_allocated;
};


union item_u {
    title_chunk_header_t empty_header;
    large_title_chunk_t  large_title;
    small_title_chunk_t  small_title;
};



#define lc_title  __lc.__lc_title
#define lc_body   __lc.__lc_body
#define lc_broken __lc.__lc_broken
#define lc_free   __lc.__lc_free
struct large_chunk_s {
    /* we could be one of 4 things:
     *  1) a large chunk (lc) title.
     *  2) a large chunk (lc) body.
     *  3) a set of small (sc) titles & bodies.
     *  4) a free chunk. */
    union {
        large_title_chunk_t  __lc_title;
        large_body_chunk_t   __lc_body;
        large_broken_chunk_t __lc_broken;
        large_free_chunk_t  __lc_free;
    } PACKED __lc;

    LARGE_CHUNK_TAIL;
} PACKED;

union chunk_u {
    large_chunk_t lc;
    small_chunk_t sc;
};


typedef struct flat_storage_info_s flat_storage_info_t;
struct flat_storage_info_s {
    void* mmap_start;                   // start of the mmap'ed region.
    large_chunk_t* flat_storage_start;  // start of the storage region.
    large_chunk_t* uninitialized_start; // start of the uninitialized region.
    size_t unused_memory;               // unused memory region.

    // large chunk free list
    large_chunk_t* large_free_list;     // free list head.
    size_t large_free_list_sz;          // number of large free list chunks.

    // small chunk free list
    small_chunk_t* small_free_list;     // free list head.
    size_t small_free_list_sz;          // number of small free list chunks.

    // LRU.
    item* lru_head;
    item* lru_tail;

    bool initialized;

    struct {
        uint64_t large_title_chunks;
        uint64_t large_body_chunks;
        uint64_t large_broken_chunks;
        uint64_t small_title_chunks;
        uint64_t small_body_chunks;
        uint64_t broken_chunk_histogram[SMALL_CHUNKS_PER_LARGE_CHUNK + 1];

        uint64_t break_events;
        uint64_t unbreak_events;

        uint64_t migrates;
    } stats;
};


extern flat_storage_info_t fsi;


/* memset a region to a special value if NDEBUG is not defined. */
static inline void DEBUG_CLEAR(void* ptr, const size_t bytes) {
#if !defined(NDEBUG)
    memset(ptr, 0x5a, bytes);
#endif /* #if !defined(NDEBUG) */
}


static inline bool is_large_chunk(const size_t nkey, const size_t nbytes) {
    size_t small_chunks_max_size;

    // calculate how many bytes (SMALL_CHUNKS_PER_LARGE_CHUNK - 1) small chunks
    // can hold.  any larger and it is simpler and better to use a large chunk.
    // note that one of the small chunks is taken up by the header.
    small_chunks_max_size = SMALL_TITLE_CHUNK_DATA_SZ +
        (SMALL_BODY_CHUNK_DATA_SZ * (SMALL_CHUNKS_PER_LARGE_CHUNK - 2));
    if (nkey + nbytes > small_chunks_max_size) {
        return true;
    }

    return false;
}


static inline bool is_item_large_chunk(const item* it) {
    return is_large_chunk(it->empty_header.nkey, it->empty_header.nbytes);
}


static inline size_t chunks_needed(const size_t nkey, const size_t nbytes) {
    size_t total_bytes = nkey + nbytes;
    if (is_large_chunk(nkey, nbytes)) {
        /* large chunk */
        if (total_bytes < LARGE_TITLE_CHUNK_DATA_SZ) {
            return 1;
        }

        total_bytes -= LARGE_TITLE_CHUNK_DATA_SZ;
        return 1 + ((total_bytes + LARGE_BODY_CHUNK_DATA_SZ - 1) /
                    LARGE_BODY_CHUNK_DATA_SZ);
    } else {
        /* large chunk */
        if (total_bytes < SMALL_TITLE_CHUNK_DATA_SZ) {
            return 1;
        }

        total_bytes -= SMALL_TITLE_CHUNK_DATA_SZ;
        return 1 + ((total_bytes + SMALL_BODY_CHUNK_DATA_SZ - 1) /
                    SMALL_BODY_CHUNK_DATA_SZ);
    }
}


/* returns the number of chunks in the item. */
static inline size_t chunks_in_item(const item* it) {
    return chunks_needed(it->empty_header.nkey, it->empty_header.nbytes);
}


/* returns the number of chunks in the item. */
static inline size_t data_chunks_in_item(const item* it) {
    size_t count = chunks_in_item(it);
    size_t key_only_chunks;
    size_t title_data_sz;

    /* if we have no data, return 0. */
    if (it->empty_header.nbytes == 0) {
        return 0;
    }

    /* exclude chunks taken up entirely by the key */
    if (is_item_large_chunk(it)) {
        title_data_sz = LARGE_TITLE_CHUNK_DATA_SZ;
        if (it->empty_header.nkey < title_data_sz) {
            key_only_chunks = 0;
        } else {
            key_only_chunks = 1 + ((it->empty_header.nkey - LARGE_TITLE_CHUNK_DATA_SZ) / LARGE_BODY_CHUNK_DATA_SZ);
        }
    } else  {
        title_data_sz = SMALL_TITLE_CHUNK_DATA_SZ;
        if (it->empty_header.nkey < title_data_sz) {
            key_only_chunks = 0;
        } else {
            key_only_chunks = 1 + ((it->empty_header.nkey - SMALL_TITLE_CHUNK_DATA_SZ) / SMALL_BODY_CHUNK_DATA_SZ);
        }
    }

    count -= key_only_chunks;

    return count;
}


static inline size_t slackspace(const size_t nkey, const size_t nbytes) {
    size_t item_sz = nkey + nbytes;

    if (is_large_chunk(nkey, nbytes)) {
        if (item_sz < LARGE_TITLE_CHUNK_DATA_SZ) {
            return LARGE_TITLE_CHUNK_DATA_SZ - item_sz;
        } else {
            size_t additional_chunks;
            item_sz -= LARGE_TITLE_CHUNK_DATA_SZ;
            additional_chunks = (item_sz + LARGE_BODY_CHUNK_DATA_SZ - 1) / LARGE_BODY_CHUNK_DATA_SZ;

            return (additional_chunks * LARGE_BODY_CHUNK_DATA_SZ) - item_sz;
        }
    } else {
        if (item_sz < SMALL_TITLE_CHUNK_DATA_SZ) {
            return SMALL_TITLE_CHUNK_DATA_SZ - item_sz;
        } else {
            size_t additional_chunks;
            item_sz -= SMALL_TITLE_CHUNK_DATA_SZ;
            additional_chunks = (item_sz + SMALL_BODY_CHUNK_DATA_SZ - 1) / SMALL_BODY_CHUNK_DATA_SZ;

            return (additional_chunks * SMALL_BODY_CHUNK_DATA_SZ) - item_sz;
        }
    }
}


static inline size_t item_slackspace(item* it) {
    return slackspace(it->empty_header.nkey, it->empty_header.nbytes);
}


/**
 * this takes a chunkptr_t and translates it to a chunk address.
 */
static inline chunk_t* get_chunk_address(chunkptr_t chunkptr) {
    intptr_t retval = (intptr_t) fsi.flat_storage_start;
    intptr_t remainder;

    if (chunkptr == NULL_CHUNKPTR) {
        return NULL;
    }

    chunkptr --;                        /* offset by 1 so 0 has special meaning. */
    remainder = chunkptr % (LARGE_CHUNK_SZ / CHUNK_ADDRESSING_SZ);

    retval += ( (chunkptr - remainder) * CHUNK_ADDRESSING_SZ );
    retval += ( remainder * SMALL_CHUNK_SZ );
    return (chunk_t*) retval;
}


/**
 * this takes a chunk address and translates it to a chunkptr_t.
 */
static inline chunkptr_t get_chunkptr(const chunk_t* _addr) {
    intptr_t addr = (intptr_t) _addr;
    intptr_t diff = addr - ((intptr_t) fsi.flat_storage_start);
    intptr_t large_chunk_index = diff / LARGE_CHUNK_SZ;
    intptr_t remainder = diff % LARGE_CHUNK_SZ;
    chunkptr_t retval;

    if (_addr == NULL) {
        return NULL_CHUNKPTR;
    }

    assert(addr >= (intptr_t) fsi.flat_storage_start);
    assert(addr < (intptr_t) fsi.uninitialized_start);

    retval = large_chunk_index * (LARGE_CHUNK_SZ / CHUNK_ADDRESSING_SZ);

    if (remainder == 0) {
        /* either pointing to a large chunk ptr, or the first small chunk of a
         * large chunk */
    } else {
        assert(remainder % SMALL_CHUNK_SZ == 0);
        retval += (remainder / SMALL_CHUNK_SZ);
    }
    retval ++;                       /* offset by 1 so 0 has special meaning. */
    return retval;
}


static inline large_chunk_t* get_parent_chunk(small_chunk_t* small) {
    intptr_t addr = (intptr_t) small;
    intptr_t diff = addr - ((intptr_t) fsi.flat_storage_start);
    intptr_t large_chunk_index = diff / LARGE_CHUNK_SZ;
    intptr_t large_chunk_addr = (large_chunk_index * LARGE_CHUNK_SZ) +
        (intptr_t) fsi.flat_storage_start;
    large_chunk_t* lc = (large_chunk_t*) large_chunk_addr;

    assert(lc->flags == (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_USED |
                         LARGE_CHUNK_BROKEN));

    return lc;
}


static inline const large_chunk_t* get_parent_chunk_const(const small_chunk_t* small) {
    return (const large_chunk_t*) get_parent_chunk( (small_chunk_t*) small );
}


/* the following are a set of abstractions to remove casting from flat_storage.c */
static inline item* get_item_from_small_title(small_title_chunk_t* small_title) {
    return (item*) small_title;
}

static inline item* get_item_from_large_title(large_title_chunk_t* large_title) {
    return (item*) large_title;
}

static inline item* get_item_from_chunk(chunk_t* chunk) {
    item* it = (item*) chunk;

    if (it != NULL) {
        assert( is_item_large_chunk(it) ?
                (chunk->lc.flags == (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_USED | LARGE_CHUNK_TITLE)) :
                (chunk->sc.flags == (SMALL_CHUNK_INITIALIZED | SMALL_CHUNK_USED | SMALL_CHUNK_TITLE)) );
    }

    return it;
}

static inline chunk_t* get_chunk_from_item(item* it) {
    return (chunk_t*) it;
}


static inline chunk_t* get_chunk_from_small_chunk(small_chunk_t* sc) {
    return (chunk_t*) sc;
}


static inline const chunk_t* get_chunk_from_small_chunk_const(const small_chunk_t* sc) {
    return (const chunk_t*) sc;
}


static inline item*          ITEM(item_ptr_t iptr)   { return get_item_from_chunk(get_chunk_address( (chunkptr_t) iptr)); }
static inline item_ptr_t     ITEM_PTR(item* it)      { return (item_ptr_t) get_chunkptr(get_chunk_from_item(it)); }
static inline bool           ITEM_PTR_IS_NULL(item_ptr_t iptr)    { return iptr != NULL_ITEM_PTR; }

static inline uint8_t        ITEM_nkey(const item* it) { return it->empty_header.nkey; }
static inline int            ITEM_nbytes(item* it)   { return it->empty_header.nbytes; }
static inline size_t         ITEM_ntotal(item* it)   {
    if (is_item_large_chunk(it)) {
        size_t item_sz = it->empty_header.nkey + it->empty_header.nbytes;

        if (item_sz < LARGE_TITLE_CHUNK_DATA_SZ) {
            return sizeof(large_chunk_t);
        } else {
            size_t additional_chunks;
            item_sz -= LARGE_TITLE_CHUNK_DATA_SZ;
            additional_chunks = (item_sz + LARGE_BODY_CHUNK_DATA_SZ - 1) / LARGE_BODY_CHUNK_DATA_SZ;

            return sizeof(large_chunk_t) * (additional_chunks + 1);
        }
    } else {
        size_t item_sz = it->empty_header.nkey + it->empty_header.nbytes;

        if (item_sz < SMALL_TITLE_CHUNK_DATA_SZ) {
            return sizeof(small_chunk_t);
        } else {
            size_t additional_chunks;
            item_sz -= SMALL_TITLE_CHUNK_DATA_SZ;
            additional_chunks = (item_sz + SMALL_BODY_CHUNK_DATA_SZ - 1) / SMALL_BODY_CHUNK_DATA_SZ;

            return sizeof(small_chunk_t) * (additional_chunks + 1);
        }
    }
}

static inline unsigned int   ITEM_flags(item* it)    { return it->empty_header.flags; }
static inline rel_time_t     ITEM_time(item* it)     { return it->empty_header.time; }
static inline rel_time_t     ITEM_exptime(item* it)  { return it->empty_header.exptime; }
static inline unsigned short ITEM_refcount(item* it) { return it->empty_header.refcount; }

static inline void ITEM_set_nbytes(item* it, int nbytes)    { it->empty_header.nbytes = nbytes; }
static inline void ITEM_set_exptime(item* it, rel_time_t t) { it->empty_header.exptime = t; }

static inline item_ptr_t ITEM_PTR_h_next(item_ptr_t iptr)  { return ITEM(iptr)->empty_header.h_next; }
static inline item_ptr_t* ITEM_h_next_p(item* it)               { return &it->empty_header.h_next; }

static inline void   ITEM_set_h_next(item* it, item_ptr_t next) { it->empty_header.h_next = next; }

static inline bool ITEM_is_valid(item* it)        { return it->empty_header.it_flags & ITEM_VALID; }
static inline bool ITEM_has_timestamp(item* it)   { return it->empty_header.it_flags & ITEM_HAS_TIMESTAMP; }
static inline bool ITEM_has_ip_address(item* it)  { return it->empty_header.it_flags & ITEM_HAS_IP_ADDRESS; }

static inline void ITEM_mark_deleted(item* it)    { it->empty_header.it_flags |= ITEM_DELETED; }
static inline void ITEM_unmark_deleted(item* it)  { it->empty_header.it_flags &= ~ITEM_DELETED; }
static inline void ITEM_set_has_timestamp(item* it)      { it->empty_header.it_flags |= ITEM_HAS_TIMESTAMP; }
static inline void ITEM_clear_has_timestamp(item* it)    { it->empty_header.it_flags &= ~(ITEM_HAS_TIMESTAMP); }
static inline void ITEM_set_has_ip_address(item* it)     { it->empty_header.it_flags |= ITEM_HAS_IP_ADDRESS; }
static inline void ITEM_clear_has_ip_address(item* it)   { it->empty_header.it_flags &= ~(ITEM_HAS_IP_ADDRESS); }

extern void flat_storage_init(size_t maxbytes);
extern char* do_item_cachedump(const chunk_type_t type, const unsigned int limit, unsigned int *bytes);
extern const char* item_key_copy(const item* it, char* keyptr);

DECL_MT_FUNC(char*, flat_allocator_stats, (size_t* bytes));

FA_STATIC_DECL(bool flat_storage_alloc(void));
FA_STATIC_DECL(item* get_lru_item(void));


static inline size_t __fs_MIN(size_t a, size_t b) {
    if (a < b) {
        return a;
    } else {
        return b;
    }
}

static inline size_t __fs_MAX(size_t a, size_t b) {
    if (a > b) {
        return a;
    } else {
        return b;
    }
}


/* this macro walks over the item and calls applier with the following
 * arguments:
 * applier(it, ptr, bytes)
 * where: it - the item object
 *        ptr - the pointer to the data
 *        bytes - the number of bytes after ptr that contain the requested bytes
 */
#define ITEM_WALK(_it, _offset, _nbytes, _beyond_item_boundary, applier, _const) \
    do {                                                                \
        chunk_t* next;                                                  \
        _const char* ptr;                                               \
        size_t to_scan;                     /* bytes left in current chunk. */ \
        /* these are the offsets to the start of the data value. */     \
        size_t start_offset, end_offset;                                \
        size_t left = (_nbytes);               /* bytes left to copy */ \
                                                                        \
        assert((_it)->empty_header.nkey + (_it)->empty_header.nbytes >= \
               (_offset) + (_nbytes) || (_beyond_item_boundary));       \
                                                                        \
        /* if we have no to copy, then skip. */                         \
        if (left == 0) {                                                \
            break;                                                      \
        }                                                               \
                                                                        \
        if (is_item_large_chunk((_it))) {                               \
            /* large chunk handling code. */                            \
                                                                        \
            next = get_chunk_address((_it)->empty_header.next_chunk);   \
            ptr = &(_it)->large_title.data[0];                          \
            start_offset = 0;                                           \
            if (next == NULL && (_beyond_item_boundary)) {              \
                end_offset = LARGE_TITLE_CHUNK_DATA_SZ - 1;             \
            } else {                                                    \
                end_offset = __fs_MIN((_offset) + (_nbytes),            \
                                       start_offset + LARGE_TITLE_CHUNK_DATA_SZ) - 1; \
            }                                                           \
            to_scan = end_offset - start_offset + 1;                    \
                                                                        \
            /* advance over pages writing while doing the requested action. */ \
            do {                                                        \
                /* offset must be less than end offset. */              \
                if ((_offset) <= end_offset) {                          \
                    /* we have some work to do. */                      \
                                                                        \
                    size_t work_start, work_end, work_len;              \
                                                                        \
                    work_start = __fs_MAX((_offset), start_offset);     \
                    work_end = __fs_MIN((_offset) + (_nbytes) - 1, end_offset); \
                    work_len = work_end - work_start + 1;               \
                                                                        \
                    applier((_it), ptr + work_start - start_offset, work_len); \
                    left -= work_len;                                   \
                }                                                       \
                                                                        \
                if (left == 0) {                                        \
                    break;                                              \
                }                                                       \
                                                                        \
                start_offset += to_scan;                                \
                                                                        \
                assert(next != NULL);                                   \
                assert( (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_USED) == next->lc.flags ); \
                ptr = next->lc.lc_body.data;                            \
                next = get_chunk_address(next->lc.lc_body.next_chunk);  \
                if (next == NULL &&                                     \
                    (_beyond_item_boundary)) {                          \
                    end_offset = start_offset + LARGE_BODY_CHUNK_DATA_SZ - 1; \
                } else {                                                \
                    end_offset = __fs_MIN((_offset) + (_nbytes),        \
                                           start_offset + LARGE_BODY_CHUNK_DATA_SZ) - 1; \
                }                                                       \
                to_scan = end_offset - start_offset + 1;                \
            } while (start_offset <= ((_it)->empty_header.nkey +        \
                                      (_it)->empty_header.nbytes));     \
        } else {                                                        \
            /* small chunk handling code. */                            \
                                                                        \
            next = get_chunk_address((_it)->empty_header.next_chunk);   \
            ptr = &(_it)->small_title.data[0];                          \
            start_offset = 0;                                           \
            if (next == NULL && (_beyond_item_boundary)) {              \
                end_offset = SMALL_TITLE_CHUNK_DATA_SZ - 1;             \
            } else {                                                    \
                end_offset = __fs_MIN((_offset) + (_nbytes),            \
                                       start_offset + SMALL_TITLE_CHUNK_DATA_SZ) - 1; \
            }                                                           \
            to_scan = end_offset - start_offset + 1;                    \
                                                                        \
            /* advance over pages writing while doing the requested action. */ \
            do {                                                        \
                /* offset must be less than end offset. */              \
                if ((_offset) <= end_offset) {                          \
                    /* we have some work to do. */                      \
                                                                        \
                    size_t work_start, work_end, work_len;              \
                                                                        \
                    work_start = __fs_MAX((_offset), start_offset);     \
                    work_end = __fs_MIN((_offset) + (_nbytes) - 1, end_offset); \
                    work_len = work_end - work_start + 1;               \
                                                                        \
                    applier((_it), ptr + work_start - start_offset, work_len); \
                    left -= work_len;                                   \
                }                                                       \
                                                                        \
                if (left == 0) {                                        \
                    break;                                              \
                }                                                       \
                                                                        \
                start_offset += to_scan;                                \
                                                                        \
                assert(next != NULL);                                   \
                assert( (SMALL_CHUNK_INITIALIZED | SMALL_CHUNK_USED) == next->sc.flags ); \
                ptr = next->sc.sc_body.data;                            \
                next = get_chunk_address(next->sc.sc_body.next_chunk);  \
                if (next == NULL &&                                     \
                    (_beyond_item_boundary)) {                          \
                    end_offset = start_offset + SMALL_BODY_CHUNK_DATA_SZ - 1; \
                } else {                                                \
                    end_offset = __fs_MIN((_offset) + (_nbytes),        \
                                           start_offset + SMALL_BODY_CHUNK_DATA_SZ) - 1; \
                }                                                       \
                /* printf("  cycling start_offset = %ld, end_offset = %ld\n", start_offset, end_offset); */ \
                to_scan = end_offset - start_offset + 1;                \
            } while (start_offset <= ((_it)->empty_header.nkey +        \
                                      (_it)->empty_header.nbytes));     \
        }                                                               \
        assert(left == 0);                                              \
    } while (0);

#endif /* #if !defined(_flat_storage_h_) */
