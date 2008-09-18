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
#include "conn_buffer.h"

static inline int add_item_value_to_iov(conn *c, const item* it, bool send_cr_lf) {
    int retval;

#define ADD_ITEM_TO_IOV_APPLIER(it, ptr, bytes)                 \
    if ((retval = add_iov(c, (ptr), (bytes), false)) != 0) {    \
        return retval;                                          \
    }

    ITEM_WALK(it, it->empty_header.nkey, it->empty_header.nbytes, false, ADD_ITEM_TO_IOV_APPLIER, const);

#undef ADD_ITEM_TO_IOV_APPLIER

    if (send_cr_lf) {
        return add_iov(c, "\r\n", 2, false);
    } else {
        return 0;
    }
}


static inline int add_item_key_to_iov(conn *c, const item* it) {
    int retval;

#define ADD_ITEM_TO_IOV_APPLIER(it, ptr, bytes)                 \
    if ((retval = add_iov(c, (ptr), (bytes), false)) != 0) {    \
        return retval;                                          \
    }

    ITEM_WALK(it, 0, it->empty_header.nkey, false, ADD_ITEM_TO_IOV_APPLIER, const);

#undef ADD_ITEM_TO_IOV_APPLIER

    return 0;
}


static inline size_t item_setup_receive(item* it, conn* c) {
    struct iovec* current_iov;
    size_t iov_len_required = data_chunks_in_item(it);

    assert(sizeof(struct iovec) * iov_len_required <= CONN_BUFFER_DATA_SZ);

    if (c->binary == false) {
        iov_len_required ++;            /* to accomodate the cr-lf */

        assert(c->riov == NULL);
        assert(c->riov_size == 0);
        c->riov = (struct iovec*) alloc_conn_buffer(c->cbg, sizeof(struct iovec) * iov_len_required);
        if (c->riov == NULL) {
            return false;
        }
    }
    /* in binary protocol, receiving the key already requires the riov to be set
     * up. */

    report_max_rusage(c->cbg, c->riov, sizeof(struct iovec) * iov_len_required);
    c->riov_size = iov_len_required;
    c->riov_left = iov_len_required;
    c->riov_curr = 0;

    current_iov = c->riov;

#define ITEM_SETUP_RECEIVE_APPLIER(it, ptr, bytes)  \
    current_iov->iov_base = ptr;                    \
    current_iov->iov_len = bytes;                   \
    current_iov ++;

    ITEM_WALK(it, it->empty_header.nkey, it->empty_header.nbytes, false, ITEM_SETUP_RECEIVE_APPLIER, )

#undef ITEM_SETUP_RECEIVE_APPLIER

    if (c->binary == false) {
        current_iov->iov_base = c->crlf;
        current_iov->iov_len = 2;
        current_iov ++;
    }

    assert(current_iov - c->riov == iov_len_required);

    return true;
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

    ITEM_WALK(it, it->empty_header.nkey, it->empty_header.nbytes, false, ITEM_STRTOUL_APPLIER, const)

#undef ITEM_STRTOUL_APPLIER

    return value;
}


static inline void item_memset(item* it, size_t offset, int c, size_t nbytes) {
#define MEMSET_APPLIER(it, ptr, bytes)       \
    memset((ptr), c, bytes);

    ITEM_WALK(it, it->empty_header.nkey + offset, nbytes, 0, MEMSET_APPLIER, );
#undef MEMSETAPPLIER
}


#endif /* #if !defined(_flat_storage_support_h_) */
#endif /* #if defined(USE_FLAT_ALLOCATOR) */
