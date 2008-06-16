/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/**
 * the contents of this file must be separate from slabs_items.h because it
 * depends on the full contents of memcached.h.  the last part of memcached.h
 * does not get processed until after slabs_items.h gets processed, so we have
 * to do it separately.
 */

#if defined(USE_SLAB_ALLOCATOR)
#if !defined(_slabs_items_support_h_)
#define _slabs_items_support_h_

#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#include "generic.h"

#include "slabs_items.h"
#include "memcached.h"


#define ITEM_data(item)   ((char*) &((item)->end) + (item)->nkey)


static inline int add_item_to_iov(conn *c, const item* it, bool send_cr_lf) {
    if (send_cr_lf) {
        return (add_iov(c, ITEM_data(it), it->nbytes, false) ||
                add_iov(c, "\r\n", 2, false));
    } else {
        return add_iov(c, ITEM_data(it), it->nbytes, false);
    }
}


static inline bool item_setup_receive(item* it, conn* c) {
    size_t iov_len_required;

    if (c->binary == false) {
        iov_len_required = 2;

        assert(c->riov == NULL);
        assert(c->riov_size == 0);
        c->riov = (struct iovec*) alloc_conn_buffer(sizeof(struct iovec) * iov_len_required);
        if (c->riov == NULL) {
            return false;
        }
    } else {
        iov_len_required = 1;
    }

    report_max_rusage(c->riov, sizeof(struct iovec) * iov_len_required);
    c->riov_size = iov_len_required;
    c->riov_left = iov_len_required;
    c->riov_curr = 0;

    c->riov[0].iov_base = ITEM_data(it);
    c->riov[0].iov_len = it->nbytes;

    if (c->binary) {
        return true;
    } else {
        c->riov[1].iov_base = c->crlf;
        c->riov[1].iov_len = 2;

        return true;
    }
}

static inline int item_strtoul(const item* it, int base) {
    uint32_t value = 0;
    char* src;
    int i;

    for (i = 0, src = ITEM_data(it);
         i < ITEM_nbytes(it);
         i ++, src ++) {
        if (! isdigit(*src)) {
            return 0;
        } else {
            uint32_t prev_value = value;

            value = (value * 10) + (*src - '0');

            if (prev_value > value) {
                /* overflowed.  return 0. */
                return 0;
            }
        }
    }

    return value;
}


static inline void item_memset(item* it, size_t offset, int c, size_t nbytes) {
    memset(ITEM_data(it) + offset, c, nbytes);
}


// undefine the macro to resist catch inappropriate use of the macro.
#if defined(__need_ITEM_data)
#undef __need_ITEM_data
#else
#undef ITEM_data
#endif /* #if defined(__need_ITEM_data) */

#endif /* #if !defined(_slabs_items_support_h_) */
#endif /* #if defined(USE_SLAB_ALLOCATOR) */
