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

static inline int add_item_to_iov(conn_t *c, const item* it, bool send_cr_lf) {
    const chunk_t* next;
    const char* ptr;
    size_t to_copy;                     /* bytes left in the current */
                                        /* chunk. */
    size_t title_data_size;             /* this is a stupid kludge because if
                                         * we directly test nkey < 
                                         * LARGE_TITLE_CHUNK_DATA_SZ, it will
                                         * always return true.  this offends
                                         * the compiler, so here we go. */
    size_t nbytes;
    int retval;

    nbytes = it->empty_header.nbytes;

    if (is_item_large_chunk(it)) {
        /* large chunk handling code. */
        
        title_data_size = LARGE_TITLE_CHUNK_DATA_SZ;
        /* is there any data in the title block? */
        if (it->empty_header.nkey < title_data_size) {
            /* some data in the title block. */
            next = get_chunk_address(it->empty_header.next_chunk);
            ptr = &it->large_title.data[it->empty_header.nkey];
            to_copy = __fss_MIN(nbytes, LARGE_TITLE_CHUNK_DATA_SZ - (it->empty_header.nkey));
        } else {
            /* no data in the title block, that means the key is exactly the
             * same size as LARGE_TITLE_CHUNK_DATA_SZ.
             */
            next = get_chunk_address(it->empty_header.next_chunk);
            assert( (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_USED) == next->lc.flags );
            ptr = next->lc.lc_body.data;
            to_copy = __fss_MIN(nbytes, LARGE_BODY_CHUNK_DATA_SZ);

            /* move on to the next one. */
            next = get_chunk_address(next->lc.lc_body.next_chunk);
        }

        while (nbytes > 0) {
            if ((retval = add_iov(c, ptr, to_copy, false)) != 0) {
                return retval;
            }
            
            nbytes -= to_copy;

            /* break if we're done. */
            if (nbytes == 0) {
                break;
            }

            /* move to the next chunk. */
            assert(next != NULL);
            assert( (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_USED) == next->lc.flags );
            ptr = next->lc.lc_body.data;
            to_copy = __fss_MIN(nbytes, LARGE_BODY_CHUNK_DATA_SZ);
            next = get_chunk_address(next->lc.lc_body.next_chunk);
        }
    } else {
        /* small chunk handling code. */
        
        title_data_size = SMALL_TITLE_CHUNK_DATA_SZ;
        /* is there any data in the title block? */
        if (it->empty_header.nkey < title_data_size) {
            /* some data in the title block. */
            next = get_chunk_address(it->empty_header.next_chunk);
            ptr = &it->small_title.data[it->empty_header.nkey];
            to_copy = __fss_MIN(nbytes, SMALL_TITLE_CHUNK_DATA_SZ - (it->empty_header.nkey));
        } else {
            /* no data in the title block, that means the key is exactly the
             * same size as SMALL_TITLE_CHUNK_DATA_SZ.
             */
            next = get_chunk_address(it->empty_header.next_chunk);
            assert( (SMALL_CHUNK_INITIALIZED | SMALL_CHUNK_USED) == next->sc.flags );
            ptr = next->sc.sc_body.data;
            to_copy = __fss_MIN(nbytes, SMALL_BODY_CHUNK_DATA_SZ);

            /* move on to the next one. */
            next = get_chunk_address(next->sc.sc_body.next_chunk);
        }

        while (nbytes > 0) {
            if ((retval = add_iov(c, ptr, to_copy, false)) != 0) {
                return retval;
            }
            
            nbytes -= to_copy;

            /* break if we're done. */
            if (nbytes == 0) {
                break;
            }

            /* move to the next chunk. */
            assert(next != NULL);
            assert( (SMALL_CHUNK_INITIALIZED | SMALL_CHUNK_USED) == next->sc.flags );
            ptr = next->sc.sc_body.data;
            to_copy = __fss_MIN(nbytes, SMALL_BODY_CHUNK_DATA_SZ);
            next = get_chunk_address(next->sc.sc_body.next_chunk);
        }
    }

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
        1 /* for the header block */;
}

static inline size_t item_setup_receive(item* it, struct iovec* iov, bool expect_cr_lf,
                                        char* crlf) {
    chunk_t* next;
    char* ptr;
    size_t to_copy;                     /* bytes left in the current */
                                        /* chunk. */
    size_t title_data_size;             /* this is a stupid kludge because if
                                         * we directly test nkey < 
                                         * LARGE_TITLE_CHUNK_DATA_SZ, it will
                                         * always return true.  this offends
                                         * the compiler, so here we go. */
    size_t nbytes;
    struct iovec* current_iov = iov;

    nbytes = it->empty_header.nbytes;

    if (is_item_large_chunk(it)) {
        /* large chunk handling code. */
        
        title_data_size = LARGE_TITLE_CHUNK_DATA_SZ;
        /* is there any data in the title block? */
        if (it->empty_header.nkey < title_data_size) {
            /* some data in the title block. */
            next = get_chunk_address(it->empty_header.next_chunk);
            ptr = &it->large_title.data[it->empty_header.nkey];
            to_copy = __fss_MIN(nbytes, LARGE_TITLE_CHUNK_DATA_SZ - (it->empty_header.nkey));
        } else {
            /* no data in the title block, that means the key is exactly the
             * same size as LARGE_TITLE_CHUNK_DATA_SZ.
             */
            next = get_chunk_address(it->empty_header.next_chunk);
            assert( (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_USED) == next->lc.flags );
            ptr = next->lc.lc_body.data;
            to_copy = __fss_MIN(nbytes, LARGE_BODY_CHUNK_DATA_SZ);

            /* move on to the next one. */
            next = get_chunk_address(next->lc.lc_body.next_chunk);
        }

        while (nbytes > 0) {
            current_iov->iov_base = ptr;
            current_iov->iov_len = to_copy;
            current_iov ++;
            
            nbytes -= to_copy;

            /* break if we're done. */
            if (nbytes == 0) {
                break;
            }

            /* move to the next chunk. */
            assert(next != NULL);
            assert( (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_USED) == next->lc.flags );
            ptr = next->lc.lc_body.data;
            to_copy = __fss_MIN(nbytes, LARGE_BODY_CHUNK_DATA_SZ);
            next = get_chunk_address(next->lc.lc_body.next_chunk);
        }
    } else {
        /* small chunk handling code. */
        
        title_data_size = SMALL_TITLE_CHUNK_DATA_SZ;
        /* is there any data in the title block? */
        if (it->empty_header.nkey < title_data_size) {
            /* some data in the title block. */
            next = get_chunk_address(it->empty_header.next_chunk);
            ptr = &it->small_title.data[it->empty_header.nkey];
            to_copy = __fss_MIN(nbytes, SMALL_TITLE_CHUNK_DATA_SZ - (it->empty_header.nkey));
        } else {
            /* no data in the title block, that means the key is exactly the
             * same size as SMALL_TITLE_CHUNK_DATA_SZ.
             */
            next = get_chunk_address(it->empty_header.next_chunk);
            assert( (SMALL_CHUNK_INITIALIZED | SMALL_CHUNK_USED) == next->sc.flags );
            ptr = next->sc.sc_body.data;
            to_copy = __fss_MIN(nbytes, SMALL_BODY_CHUNK_DATA_SZ);

            /* move on to the next one. */
            next = get_chunk_address(next->sc.sc_body.next_chunk);
        }

        while (nbytes > 0) {
            current_iov->iov_base = ptr;
            current_iov->iov_len = to_copy;
            current_iov ++;
            
            nbytes -= to_copy;

            /* break if we're done. */
            if (nbytes == 0) {
                break;
            }

            /* move to the next chunk. */
            assert(next != NULL);
            assert( (SMALL_CHUNK_INITIALIZED | SMALL_CHUNK_USED) == next->sc.flags );
            ptr = next->sc.sc_body.data;
            to_copy = __fss_MIN(nbytes, SMALL_BODY_CHUNK_DATA_SZ);
            next = get_chunk_address(next->sc.sc_body.next_chunk);
        }
    }

    if (expect_cr_lf) {
        current_iov->iov_base = crlf;
        current_iov->iov_len = 2;
        current_iov ++;
    }
    
    assert(current_iov - iov <= item_get_max_riov());
    return current_iov - iov;           /* number of IOVs written. */
}

static inline int item_strtoul(const item* it, int base) {
    const chunk_t* next;
    const char* ptr;
    size_t to_copy;                     /* bytes left in the current */
                                        /* chunk. */
    size_t title_data_size;             /* this is a stupid kludge because if
                                         * we directly test nkey < 
                                         * LARGE_TITLE_CHUNK_DATA_SZ, it will
                                         * always return true.  this offends
                                         * the compiler, so here we go. */
    size_t nbytes = it->empty_header.nbytes;
    uint32_t value = 0;

    if (is_item_large_chunk(it)) {
        /* large chunk handling code. */
        
        title_data_size = LARGE_TITLE_CHUNK_DATA_SZ;
        /* is there any data in the title block? */
        if (it->empty_header.nkey < title_data_size) {
            /* some data in the title block. */
            next = get_chunk_address(it->empty_header.next_chunk);
            ptr = &it->large_title.data[it->empty_header.nkey];
            to_copy = __fss_MIN(nbytes, LARGE_TITLE_CHUNK_DATA_SZ - (it->empty_header.nkey));
        } else {
            /* no data in the title block, that means the key is exactly the
             * same size as LARGE_TITLE_CHUNK_DATA_SZ.
             */
            next = get_chunk_address(it->empty_header.next_chunk);
            assert( (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_USED) == next->lc.flags );
            ptr = next->lc.lc_body.data;
            to_copy = __fss_MIN(nbytes, LARGE_TITLE_CHUNK_DATA_SZ);

            /* move on to the next one. */
            next = get_chunk_address(next->lc.lc_body.next_chunk);
        }

        while (nbytes > 0) {
            size_t i;

            for (i = 0;
                 i < to_copy;
                 i ++, ptr ++) {
                if (! isdigit(*ptr)) {
                    return 0;
                } else {
                    uint32_t prev_value = value;

                    value = (value * 10) + (*ptr - '0');

                    if (prev_value > value) {
                        /* overflowed.  return 0. */
                        return 0;
                    }
                }
            }
            
            nbytes -= to_copy;

            /* break if we're done. */
            if (nbytes == 0) {
                break;
            }

            /* move to the next chunk. */
            assert(next != NULL);
            assert( (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_USED) == next->lc.flags );
            ptr = next->lc.lc_body.data;
            to_copy = __fss_MIN(nbytes, LARGE_BODY_CHUNK_DATA_SZ);
            next = get_chunk_address(next->lc.lc_body.next_chunk);
        }
    } else {
        /* small chunk handling code. */
        
        title_data_size = SMALL_TITLE_CHUNK_DATA_SZ;
        /* is there any data in the title block? */
        if (it->empty_header.nkey < title_data_size) {
            /* some data in the title block. */
            next = get_chunk_address(it->empty_header.next_chunk);
            ptr = &it->small_title.data[it->empty_header.nkey];
            to_copy = __fss_MIN(nbytes, SMALL_TITLE_CHUNK_DATA_SZ - (it->empty_header.nkey));
        } else {
            /* no data in the title block, that means the key is exactly the
             * same size as SMALL_TITLE_CHUNK_DATA_SZ.
             */
            next = get_chunk_address(it->empty_header.next_chunk);
            assert( (SMALL_CHUNK_INITIALIZED | SMALL_CHUNK_USED) == next->sc.flags );
            ptr = next->sc.sc_body.data;
            to_copy = __fss_MIN(nbytes, SMALL_TITLE_CHUNK_DATA_SZ);

            /* move on to the next one. */
            next = get_chunk_address(next->sc.sc_body.next_chunk);
        }

        while (nbytes > 0) {
            size_t i;

            for (i = 0;
                 i < to_copy;
                 i ++, ptr ++) {
                if (! isdigit(*ptr)) {
                    return 0;
                } else {
                    uint32_t prev_value = value;

                    value = (value * 10) + (*ptr - '0');

                    if (prev_value > value) {
                        /* overflowed.  return 0. */
                        return 0;
                    }
                }
            }
            
            nbytes -= to_copy;

            /* break if we're done. */
            if (nbytes == 0) {
                break;
            }

            /* move to the next chunk. */
            assert(next != NULL);
            assert( (SMALL_CHUNK_INITIALIZED | SMALL_CHUNK_USED) == next->sc.flags );
            ptr = next->sc.sc_body.data;
            to_copy = __fss_MIN(nbytes, SMALL_BODY_CHUNK_DATA_SZ);
            next = get_chunk_address(next->sc.sc_body.next_chunk);
        }
    }

    return value;
}

static inline void item_memcpy_to(item* it, const void* src, size_t nbytes) {
    chunk_t* next;
    char* ptr;
    size_t to_copy;                     /* bytes left in the current */
                                        /* chunk. */
    size_t title_data_size;             /* this is a stupid kludge because if
                                         * we directly test nkey < 
                                         * LARGE_TITLE_CHUNK_DATA_SZ, it will
                                         * always return true.  this offends
                                         * the compiler, so here we go. */

    assert(it->empty_header.nbytes >= nbytes);

    if (is_item_large_chunk(it)) {
        /* large chunk handling code. */
        
        title_data_size = LARGE_TITLE_CHUNK_DATA_SZ;
        /* is there any data in the title block? */
        if (it->empty_header.nkey < title_data_size) {
            /* some data in the title block. */
            next = get_chunk_address(it->empty_header.next_chunk);
            ptr = &it->large_title.data[it->empty_header.nkey];
            to_copy = __fss_MIN(nbytes, LARGE_TITLE_CHUNK_DATA_SZ - (it->empty_header.nkey));
        } else {
            /* no data in the title block, that means the key is exactly the
             * same size as LARGE_TITLE_CHUNK_DATA_SZ.
             */
            next = get_chunk_address(it->empty_header.next_chunk);
            assert( (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_USED) == next->lc.flags );
            ptr = next->lc.lc_body.data;
            to_copy = __fss_MIN(nbytes, LARGE_TITLE_CHUNK_DATA_SZ);

            /* move on to the next one. */
            next = get_chunk_address(next->lc.lc_body.next_chunk);
        }

        while (nbytes > 0) {
            memcpy(ptr, src, to_copy);
            
            src += to_copy;
            nbytes -= to_copy;

            /* break if we're done. */
            if (nbytes == 0) {
                break;
            }

            /* move to the next chunk. */
            assert(next != NULL);
            assert( (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_USED) == next->lc.flags );
            ptr = next->lc.lc_body.data;
            to_copy = __fss_MIN(nbytes, LARGE_BODY_CHUNK_DATA_SZ);
            next = get_chunk_address(next->lc.lc_body.next_chunk);
        }
    } else {
        /* small chunk handling code. */

        title_data_size = SMALL_TITLE_CHUNK_DATA_SZ;        
        /* is there any data in the title block? */
        if (it->empty_header.nkey < title_data_size) {
            /* some data in the title block. */
            next = get_chunk_address(it->empty_header.next_chunk);
            ptr = &it->small_title.data[it->empty_header.nkey];
            to_copy = __fss_MIN(nbytes, SMALL_TITLE_CHUNK_DATA_SZ - (it->empty_header.nkey));
        } else {
            /* no data in the title block, that means the key is exactly the
             * same size as SMALL_TITLE_CHUNK_DATA_SZ.
             */
            next = get_chunk_address(it->empty_header.next_chunk);
            assert( (SMALL_CHUNK_INITIALIZED | SMALL_CHUNK_USED) == next->sc.flags );
            ptr = next->sc.sc_body.data;
            to_copy = __fss_MIN(nbytes, SMALL_BODY_CHUNK_DATA_SZ);

            /* move on to the next one. */
            next = get_chunk_address(next->sc.sc_body.next_chunk);
        }

        while (nbytes > 0) {
            memcpy(ptr, src, to_copy);
            
            src += to_copy;
            nbytes -= to_copy;

            /* break if we're done. */
            if (nbytes == 0) {
                break;
            }

            /* move to the next chunk. */
            assert(next != NULL);
            assert( (SMALL_CHUNK_INITIALIZED | SMALL_CHUNK_USED) == next->sc.flags );
            ptr = next->sc.sc_body.data;
            to_copy = __fss_MIN(nbytes, SMALL_BODY_CHUNK_DATA_SZ);
            next = get_chunk_address(next->sc.sc_body.next_chunk);
        }
    }
}

static inline void item_memset(item* it, size_t offset, int c, size_t nbytes) {
    chunk_t* next;
    char* ptr;
    size_t to_scan;                     /* bytes left in current chunk. */
    size_t start_offset, end_offset;    /* these are the offsets (from the start
                                         * of the value segment) to the start of
                                         * the data value. */
    size_t left;                        /* bytes left in the item */
    size_t title_data_size;             /* this is a stupid kludge because if
                                         * we directly test nkey < 
                                         * LARGE_TITLE_CHUNK_DATA_SZ, it will
                                         * always return true.  this offends
                                         * the compiler, so here we go. */

    assert(it->empty_header.nbytes >= offset + nbytes);

    left = it->empty_header.nbytes;
    if (is_item_large_chunk(it)) {
        /* large chunk handling code. */

        title_data_size = LARGE_TITLE_CHUNK_DATA_SZ;
        /* is there any data in the title block? */
        if (it->empty_header.nkey < title_data_size) {
            /* some data in the title block. */
            next = get_chunk_address(it->empty_header.next_chunk);
            ptr = &it->large_title.data[it->empty_header.nkey];
            start_offset = 0;
            end_offset = __fss_MIN(left, LARGE_TITLE_CHUNK_DATA_SZ - (it->empty_header.nkey)) - 1;
            to_scan = end_offset - start_offset + 1;
        } else {
            /* no data in the title block, that means the key is exactly the
             * same size as LARGE_TITLE_CHUNK_DATA_SZ.
             */
            next = get_chunk_address(it->empty_header.next_chunk);
            assert( (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_USED) == next->lc.flags );
            ptr = next->lc.lc_body.data;
            start_offset = 0;
            end_offset = __fss_MIN(left, LARGE_BODY_CHUNK_DATA_SZ) - 1;
            to_scan = end_offset - start_offset + 1;

            /* move on to the next one. */
            next = get_chunk_address(next->lc.lc_body.next_chunk);
        }

        /* advance over pages writing while doing the appropriate memsets. */
        do {
            /* is either offset between start_offset and end_offset, or offset +
             * nbytess - 1 between start_offset and end_offset? */
            if ( (start_offset >= offset &&
                  start_offset <= (offset + nbytes - 1)) ||
                 (end_offset >= offset &&
                  end_offset <= (offset + nbytes - 1)) ) {
                /* we have some memsetting to do. */

                size_t memset_start, memset_end, memset_len;

                memset_start = __fss_MAX(offset, start_offset);
                memset_end = __fss_MIN(offset + nbytes - 1, end_offset);
                memset_len = memset_end - memset_start + 1;

                memset(ptr + memset_start - start_offset, c, memset_len);
            }

            left -= to_scan;
            start_offset += to_scan;

            if (left == 0) {
                break;
            }

            assert(next != NULL);
            assert( (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_USED) == next->lc.flags );
            ptr = next->lc.lc_body.data;
            end_offset = start_offset + __fss_MIN(left, LARGE_BODY_CHUNK_DATA_SZ) - 1;
            to_scan = end_offset - start_offset + 1;
            next = get_chunk_address(next->lc.lc_body.next_chunk);
        } while (end_offset <= (offset + nbytes - 1));
    } else {
        /* small chunk handling code. */

        title_data_size = SMALL_TITLE_CHUNK_DATA_SZ;
        /* is there any data in the title block? */
        if (it->empty_header.nkey < title_data_size) {
            /* some data in the title block. */
            next = get_chunk_address(it->empty_header.next_chunk);
            ptr = &it->small_title.data[it->empty_header.nkey];
            start_offset = 0;
            end_offset = __fss_MIN(left, SMALL_TITLE_CHUNK_DATA_SZ - (it->empty_header.nkey)) - 1;
            to_scan = end_offset - start_offset + 1;
        } else {
            /* no data in the title block, that means the key is exactly the
             * same size as SMALL_TITLE_CHUNK_DATA_SZ.
             */
            next = get_chunk_address(it->empty_header.next_chunk);
            assert( (SMALL_CHUNK_INITIALIZED | SMALL_CHUNK_USED) == next->sc.flags );
            ptr = next->sc.sc_body.data;
            start_offset = 0;
            end_offset = __fss_MIN(left, SMALL_BODY_CHUNK_DATA_SZ) - 1;
            to_scan = end_offset - start_offset + 1;

            /* move on to the next one. */
            next = get_chunk_address(next->sc.sc_body.next_chunk);
        }

        /* advance over pages writing while doing the appropriate memsets. */
        do {
            /* is either offset between start_offset and end_offset, or offset +
             * nbytess - 1 between start_offset and end_offset? */
            if ( (start_offset >= offset &&
                  start_offset <= (offset + nbytes - 1)) ||
                 (end_offset >= offset &&
                  end_offset <= (offset + nbytes - 1)) ) {
                /* we have some memsetting to do. */

                size_t memset_start, memset_end, memset_len;

                memset_start = __fss_MAX(offset, start_offset);
                memset_end = __fss_MIN(offset + nbytes - 1, end_offset);
                memset_len = memset_end - memset_start + 1;

                memset(ptr + memset_start - start_offset, c, memset_len);
            }

            left -= to_scan;
            start_offset += to_scan;

            if (left == 0) {
                break;
            }

            assert(next != NULL);
            assert( (SMALL_CHUNK_INITIALIZED | SMALL_CHUNK_USED) == next->sc.flags );
            ptr = next->sc.sc_body.data;
            end_offset = start_offset + __fss_MIN(left, SMALL_BODY_CHUNK_DATA_SZ) - 1;
            to_scan = end_offset - start_offset + 1;
            next = get_chunk_address(next->sc.sc_body.next_chunk);
        } while (end_offset <= (offset + nbytes - 1));
    }
}

#endif /* #if !defined(_flat_storage_support_h_) */
#endif /* #if defined(USE_SLAB_ALLOCATOR) */
