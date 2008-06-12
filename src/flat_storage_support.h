/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/**
 * the contents of this file must be separate from flat_storage.h because it
 * depends on the full contents of memcached.h.  the last part of memcached.h
 * does not get processed until after flat_storage.h gets processed, so we have
 * to do it separately.
 */

#if defined(USE_FLAT_ALLOCATOR)
#if !defined(_flat_storage_support_h_)
#define _flat_storage_support_h_

#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#include "generic.h"

#include "flat_storage.h"
#include "memcached.h"

static inline size_t __fss_MIN(size_t a, size_t b) {
    if (a < b) {
        return a;
    } else {
        return b;
    }
}

static inline size_t __fss_MAX(size_t a, size_t b) {
    if (a > b) {
        return a;
    } else {
        return b;
    }
}

static inline int add_item_to_iov(conn *c, const item* it, bool send_cr_lf) {
    int retval;

#define ADD_ITEM_TO_IOV_APPLIER(it, ptr, bytes)                 \
    if ((retval = add_iov(c, (ptr), (bytes), false)) != 0) {    \
        return retval;                                          \
    }

    ITEM_WALK(it, 0, it->empty_header.nbytes, false, ADD_ITEM_TO_IOV_APPLIER, const);

#undef ADD_ITEM_TO_IOV_APPLIER

    if (send_cr_lf) {
        return add_iov(c, "\r\n", 2, false);
    } else {
        return 0;
    }
}

/** the flat storage driver treats MAX_ITEM_SIZE the largest value we can
 * accomodate. */
static inline unsigned int item_get_max_riov(void) {
    size_t item_sz = MAX_ITEM_SIZE + KEY_MAX_LENGTH;
    if (item_sz < LARGE_TITLE_CHUNK_DATA_SZ) {
        /* can we fit in the title chunk?  really unlikely, but since these are
         * compile-time constants, testing is essentially free. */
        return 1;
    }

    /* okay, how many body chunks do we need to store the entire thing? */
    item_sz -= LARGE_TITLE_CHUNK_DATA_SZ;

    return ((item_sz + LARGE_BODY_CHUNK_DATA_SZ - 1) / LARGE_BODY_CHUNK_DATA_SZ) +
        1 /* for the header block */ + 1 /* for the cr-lf */;
}

static inline size_t item_setup_receive(item* it, struct iovec* iov, bool expect_cr_lf,
                                        char* crlf) {
    struct iovec* current_iov = iov;

#define ITEM_SETUP_RECEIVE_APPLIER(it, ptr, bytes)  \
    current_iov->iov_base = ptr;                    \
    current_iov->iov_len = bytes;                   \
    current_iov ++;

    ITEM_WALK(it, 0, it->empty_header.nbytes, false, ITEM_SETUP_RECEIVE_APPLIER, )

#undef ITEM_SETUP_RECEIVE_APPLIER

    if (expect_cr_lf) {
        current_iov->iov_base = crlf;
        current_iov->iov_len = 2;
        current_iov ++;
    }

    assert(current_iov - iov <= item_get_max_riov());
    return current_iov - iov;           /* number of IOVs written. */
}

static inline int item_strtoul(const item* it, int base) {
    uint32_t value = 0;

#define ITEM_STRTOUL_APPLIER(it, ptr, bytes)    \
    {                                           \
        size_t i;                               \
        const char* _ptr = (ptr);               \
                                                \
        for (i = 0;                                      \
             i < bytes;                                  \
             i ++) {                                     \
            if (! isdigit(_ptr[i])) {                    \
                return 0;                                \
            } else {                                     \
                uint32_t prev_value = value;             \
                                                         \
                value = (value * 10) + (_ptr[i] - '0');  \
                                                         \
                if (prev_value > value) {                \
                    /* overflowed.  return 0. */         \
                    return 0;                            \
                }                                        \
            }                                            \
        }                                                \
    }

    ITEM_WALK(it, 0, it->empty_header.nbytes, false, ITEM_STRTOUL_APPLIER, const)

#undef ITEM_STRTOUL_APPLIER

    return value;
}


static inline void item_memset(item* it, size_t offset, int c, size_t nbytes) {
#define MEMSET_APPLIER(it, ptr, bytes)       \
    memset((ptr), c, bytes);

    ITEM_WALK(it, offset, nbytes, 0, MEMSET_APPLIER, );
#undef MEMSETAPPLIER
}

#endif /* #if !defined(_flat_storage_support_h_) */
#endif /* #if defined(USE_SLAB_ALLOCATOR) */
