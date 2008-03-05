/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#if !defined(_items_support_h_)
#define _items_support_h_

#include <stdlib.h>
#include <string.h>
#include "memcached.h"
#include "generic.h"

#define ITEM_data(item)   ((char*) &((item)->end) + (item)->nkey + 1)

static inline int item_strncmp(const item* it, size_t offset, const char* ref, size_t bytes)
{
    return strncmp(ITEM_data(it) + offset, ref, bytes);
}

static inline int add_item_to_iov(conn *c, const item* it, bool send_cr_lf)
{
    if (send_cr_lf) {
        return add_iov(c, ITEM_data(it), it->nbytes, false);
    } else {
        return add_iov(c, ITEM_data(it), it->nbytes - 2, false);
    }
}

static inline unsigned int item_get_max_riov(void)
{
    return 1;
}

static inline size_t item_setup_receive(item* it, struct iovec* iov, bool expect_cr_lf)
{
    iov->iov_base = ITEM_data(it);
    if (expect_cr_lf) {
        iov->iov_len = it->nbytes;
    } else {
        iov->iov_len = it->nbytes - 2;
    }

    return 1;
}

static inline int item_strtoul(const item* it, int base)
{
    return strtoul(ITEM_data(it), NULL, base);
}

static inline void* item_memcpy_to(const item* it, size_t offset, const void* src, size_t nbytes)
{
    return memcpy(ITEM_data(it) + offset, src, nbytes);
}

static inline void* item_memset(const item* it, size_t offset, int c, size_t nbytes)
{
    return memset(ITEM_data(it) + offset, c, nbytes);
}

// undefine the macro to resist catch inappropriate use of the macro.
#undef ITEM_data

#endif /* #if !defined(_items_support_h_) */
