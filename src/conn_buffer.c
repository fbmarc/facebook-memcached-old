/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <assert.h>
#include <stdlib.h>
#include <unistd.h>

#define CONN_BUFFER_MODULE
#include "memcached.h"

#include "conn_buffer.h"

// this will enable the rigorous checking of the free list following any
// free list operation.  this could be expensive and is probably generally
// inadvisable.
// #define FREELIST_CHECK

// this will enable the rigorous detection of memory corruption bugs by
// preventing the reuse of address space previously occupied by a connection
// buffer.  connection buffers are always destroyed upon their return to the
// conn_buffer system.
// #define CONN_BUFFER_CORRUPTION_DETECTION

#define HEAP_ENTRY_TO_INDEX(level, nth)  ((1 << (level)) + (nth) - 1)
#define HEAP_PARENT(index)               ((((index) + 1) >> 1) - 1)
#define HEAP_LEFT_CHILD(index)           ((((index) + 1) << 1) - 1 + 0)
#define HEAP_RIGHT_CHILD(index)          ((((index) + 1) << 1) - 1 + 1)

#ifdef CONN_BUFFER_CORRUPTION_DETECTION
static const bool detect_corruption = true;
#else
static const bool detect_corruption = false;
#endif /* #ifdef CONN_BUFFER_CORRUPTION_DETECTION */


STATIC int cb_freelist_check(void) {
#if defined(FREELIST_CHECK)
    size_t i, found_entries, rsize_total;
    bool end_found = false;

    /* num free buffers agrees with reality? */
    for (i = 0, found_entries = 0, rsize_total = 0;
         i < cbs.free_buffer_list_capacity;
         i ++) {
        size_t left_child, right_child;

        if (cbs.free_buffers[i] == NULL) {
            end_found = true;
            continue;
        }

        assert(end_found == false);
        assert(cbs.free_buffers[i]->signature == CONN_BUFFER_SIGNATURE);
        assert(cbs.free_buffers[i]->in_freelist == true);
        assert(cbs.free_buffers[i]->used == false);
        found_entries ++;

        rsize_total += cbs.free_buffers[i]->max_rusage;

        left_child = HEAP_LEFT_CHILD(i);
        right_child = HEAP_RIGHT_CHILD(i);

        if (left_child < cbs.num_free_buffers) {
            /* valid left child */
            assert(cbs.free_buffers[left_child] != NULL);
            assert(cbs.free_buffers[left_child]->signature == CONN_BUFFER_SIGNATURE);
            assert(cbs.free_buffers[left_child]->in_freelist == true);
            assert(cbs.free_buffers[left_child]->used == false);

            assert(cbs.free_buffers[i]->max_rusage >= cbs.free_buffers[left_child]->max_rusage);
        }

        if (right_child < cbs.num_free_buffers) {
            /* valid right child */
            assert(cbs.free_buffers[right_child] != NULL);
            assert(cbs.free_buffers[right_child]->signature == CONN_BUFFER_SIGNATURE);
            assert(cbs.free_buffers[right_child]->in_freelist == true);
            assert(cbs.free_buffers[right_child]->used == false);

            assert(cbs.free_buffers[i]->max_rusage >= cbs.free_buffers[right_child]->max_rusage);
        }
    }

    assert(found_entries == cbs.num_free_buffers);
    assert(rsize_total == cbs.total_rsize_in_freelist);
#endif /* #if defined(FREELIST_CHECK) */

    return 0;
}


static size_t round_up_to_page(size_t bytes) {
    if ((bytes % cbs.settings.page_size) != 0) {
        bytes = ((bytes + cbs.settings.page_size - 1) & ~ (cbs.settings.page_size - 1));
    }

    return bytes;
}


static void add_conn_buffer_to_freelist(conn_buffer_t* buffer) {
    size_t index;

    assert(cb_freelist_check() == 0);
    (void) cb_freelist_check;      /* dummy rvalue to avoid compiler warning. */

    assert(buffer->signature == CONN_BUFFER_SIGNATURE);
    assert(buffer->in_freelist == false);
    assert(buffer->used == false);

    if (cbs.num_free_buffers >= cbs.free_buffer_list_capacity) {
        if (cbs.free_buffers == NULL) {
            cbs.free_buffer_list_capacity = cbs.settings.initial_buffer_count;
            cbs.free_buffers = (conn_buffer_t**) pool_malloc(sizeof(conn_buffer_t*) * cbs.free_buffer_list_capacity,
                                                             CONN_BUFFER_POOL);
        } else {
            cbs.free_buffers = pool_realloc(cbs.free_buffers,
                                            sizeof(conn_buffer_t*) * cbs.free_buffer_list_capacity * 2,
                                            sizeof(conn_buffer_t*) * cbs.free_buffer_list_capacity,
                                            CONN_BUFFER_POOL);
            cbs.free_buffer_list_capacity *= 2;
        }

        memset(&cbs.free_buffers[cbs.num_free_buffers], 0,
               sizeof(conn_buffer_t*) * (cbs.free_buffer_list_capacity - cbs.num_free_buffers));
    }

    buffer->in_freelist = true;

    assert(cbs.free_buffers[cbs.num_free_buffers] == NULL);
    cbs.free_buffers[cbs.num_free_buffers] = buffer;
    index = cbs.num_free_buffers;
    cbs.num_free_buffers ++;
    cbs.total_rsize_in_freelist += buffer->max_rusage;

    while (index != 0) {
        size_t parent_index = HEAP_PARENT(index);

        if (cbs.free_buffers[index]->max_rusage >
            cbs.free_buffers[parent_index]->max_rusage) {
            conn_buffer_t* temp;

            /* swap */
            temp = cbs.free_buffers[index];
            cbs.free_buffers[index] = cbs.free_buffers[parent_index];
            cbs.free_buffers[parent_index] = temp;
        } else {
            /* no swap occured, so we can stop the reheaping operation */
            break;
        }
    }
    assert(cb_freelist_check() == 0);
}


static conn_buffer_t* remove_conn_buffer_from_freelist(size_t max_rusage_hint) {
    conn_buffer_t* ret;
    conn_buffer_t* compare;
    size_t index;

    assert(cb_freelist_check() == 0);

    if (cbs.num_free_buffers == 0) {
        assert(cbs.free_buffers[0] == NULL);
        return NULL;
    }

    ret = cbs.free_buffers[0];
    cbs.free_buffers[0] = NULL;
    assert(ret->signature == CONN_BUFFER_SIGNATURE);
    assert(ret->in_freelist == true);
    assert(ret->used == false);
    ret->in_freelist = false;

    cbs.num_free_buffers --;
    cbs.total_rsize_in_freelist -= ret->max_rusage;

    if (cbs.num_free_buffers == 0) {
        assert(cb_freelist_check() == 0);
        return ret;
    }

    index = 0;
    compare = cbs.free_buffers[cbs.num_free_buffers];
    cbs.free_buffers[cbs.num_free_buffers] = NULL;

    while (true) {
        size_t left_child_index = HEAP_LEFT_CHILD(index);
        size_t right_child_index = HEAP_RIGHT_CHILD(index);
        bool valid_left, valid_right, swap_left, swap_right;

        valid_left = (left_child_index < cbs.num_free_buffers) ? true : false;
        valid_right = (right_child_index < cbs.num_free_buffers) ? true : false;

        swap_left = (valid_left &&
                     cbs.free_buffers[left_child_index]->max_rusage >
                     compare->max_rusage) ? true : false;
        swap_right = (valid_right &&
                      cbs.free_buffers[right_child_index]->max_rusage >
                      compare->max_rusage) ? true : false;

        /* it's possible that we'd want to swap with both (i.e., bigger than
         * both).  pick the larger one to swap with.
         */
        if (swap_left && swap_right) {
            if (cbs.free_buffers[left_child_index]->max_rusage >
                cbs.free_buffers[right_child_index]->max_rusage) {
                /* left is greater, swap with left. */
                swap_right = false;
            } else {
                swap_left = false;
            }
        }

        if (swap_left) {
            assert(cbs.free_buffers[index] == NULL);
            cbs.free_buffers[index] = cbs.free_buffers[left_child_index];
            cbs.free_buffers[left_child_index] = NULL;
            index = left_child_index;
        } else if (swap_right) {
            assert(cbs.free_buffers[index] == NULL);
            cbs.free_buffers[index] = cbs.free_buffers[right_child_index];
            cbs.free_buffers[right_child_index] = NULL;
            index = right_child_index;
        } else {
            assert(cbs.free_buffers[index] == NULL);
            cbs.free_buffers[index] = compare;
            break;
        }
    }

    assert(cb_freelist_check() == 0);
    return ret;
}


static conn_buffer_t* make_conn_buffer(void) {
    conn_buffer_t* buffer;

    if (cbs.total_rsize + cbs.settings.page_size >= cbs.settings.total_rsize_range_top) {
        /* we don't start the reclamation here because we didn't actually exceed
         * the top range.
         */
        return NULL;
    }

    buffer = mmap(NULL,
                  CONN_BUFFER_SIZE,
                  PROT_READ | PROT_WRITE,
                  MAP_PRIVATE | MAP_ANON,
                  -1, 0);

    if (buffer == NULL) {
        return NULL;
    }

    buffer->signature = CONN_BUFFER_SIGNATURE;
    buffer->max_rusage = round_up_to_page(CONN_BUFFER_HEADER_SZ);
    buffer->in_freelist = false;
    buffer->used = false;

    cbs.total_rsize += buffer->max_rusage;

    return buffer;
}


static bool try_remap(void* ptr, const size_t range, unsigned remap_attempts) {
    void** remaps = malloc(sizeof(void*) * remap_attempts);
    unsigned c;
    bool success = false;

    for (c = 0; c < remap_attempts; c ++) {
        remaps[c] = mmap(ptr, range, PROT_NONE, MAP_PRIVATE | MAP_ANON, -1, 0);

        if (remaps[c] == ptr) {
            success = true;
            break;
        }

        /* do the address ranges overlap? */
        if (remaps[c] >= ptr + range ||
            ptr >= remaps[c] + range) {
            /* no overlap, we're good to continue */
            continue;
        }

        /* overlap, so we can't continue. */
        break;
    }

    if (success == true) {
        /* unmap all the other mmap attempts. */
        unsigned j;

        for (j = 0; j < c; j ++) {
            munmap(remaps[j], range);
        }

        assert(remaps[j] == ptr);
    }

    free(remaps);
    return success;
}


static void destroy_conn_buffer(conn_buffer_t* buffer) {
    void* ptr = buffer;
    size_t range = buffer->max_rusage;

    assert(buffer->in_freelist == false);
    assert(buffer->used == false);
    assert(cbs.total_rsize > 0);

    cbs.stats.destroys ++;
    cbs.total_rsize -= buffer->max_rusage;
    munmap(buffer, CONN_BUFFER_SIZE);

    /* if we're trying to detect corruption, we need to freeze out the address
     * space used by the connection buffer that we're destroying. */
    if (detect_corruption) {
        void* remap = mmap(buffer, range, PROT_NONE, MAP_PRIVATE | MAP_ANON, -1, 0);
        if (remap != ptr) {
            if (! (remap >= ptr + range ||
                   ptr >= remap + range)) {
                /* overlap... can't continue */
                abort();
            }

            if (try_remap(ptr, range,
                          50 /* how many mmaps will we try to accomplish
                              * our memory corruption detection */) == false) {
                abort();
            }

            munmap(remap, range);
        }
    }
}


static conn_buffer_t* get_buffer_from_data_ptr(void* _ptr) {
    intptr_t ptr = (intptr_t) _ptr;
    conn_buffer_t* buffer;

    ptr -= CONN_BUFFER_HEADER_SZ;
    buffer = (conn_buffer_t*) ptr;

    assert(buffer->signature == CONN_BUFFER_SIGNATURE);

    return buffer;
}


void conn_buffer_init(size_t initial_buffer_count,
                      size_t buffer_rsize_limit,
                      size_t total_rsize_range_bottom,
                      size_t total_rsize_range_top) {
    size_t i;

    always_assert( cbs.initialized == false );
    always_assert( (CONN_BUFFER_HEADER_SZ % sizeof(void*)) == 0 );

    memset(&cbs, 0, sizeof(conn_buffer_status_t));

    cbs.settings.page_size = getpagesize();

    always_assert( (cbs.settings.page_size & (cbs.settings.page_size - 1)) == 0);

    /* write in some defaults */
    if (initial_buffer_count == 0) {
        initial_buffer_count = CONN_BUFFER_INITIAL_BUFFER_COUNT_DEFAULT;
    }
    if (buffer_rsize_limit == 0) {
        buffer_rsize_limit = CONN_BUFFER_RSIZE_LIMIT_DEFAULT;
    }
    if (total_rsize_range_bottom == 0) {
        total_rsize_range_bottom = CONN_BUFFER_TOTAL_RSIZE_RANGE_BOTTOM_DEFAULT;
    }
    if (total_rsize_range_top == 0) {
        total_rsize_range_top = CONN_BUFFER_TOTAL_RSIZE_RANGE_TOP_DEFAULT;
    }

    always_assert(initial_buffer_count * cbs.settings.page_size <= total_rsize_range_bottom);
    always_assert(initial_buffer_count * cbs.settings.page_size <= total_rsize_range_top);
    // always_assert(buffer_rsize_limit < total_rsize_range_bottom);
    always_assert(total_rsize_range_bottom < total_rsize_range_top);
    always_assert(buffer_rsize_limit >= cbs.settings.page_size);

    cbs.settings.initial_buffer_count = initial_buffer_count;
    cbs.settings.buffer_rsize_limit = buffer_rsize_limit;
    cbs.settings.total_rsize_range_bottom = total_rsize_range_bottom;
    cbs.settings.total_rsize_range_top = total_rsize_range_top;

    for (i = 0; i < initial_buffer_count; i ++) {
        conn_buffer_t* buffer;

        buffer = make_conn_buffer();
        always_assert(buffer != NULL);
        add_conn_buffer_to_freelist(buffer);
    }

    cbs.initialized = true;
}


void do_conn_buffer_reclamation(void) {
    if (cbs.reclamation_in_progress) {
        if (cbs.num_free_buffers != 0) {
            /* grab the most space-consuming buffer and reclaim it. */
            conn_buffer_t* tofree = remove_conn_buffer_from_freelist(CONN_BUFFER_SIZE);

            destroy_conn_buffer(tofree);
        }

        if (cbs.num_free_buffers == 0 ||
            cbs.total_rsize <= cbs.settings.total_rsize_range_bottom) {
            cbs.reclamation_in_progress = false;
        }
    }
}


/**
 * allocate a connection buffer.  max_rusage_hint is a hint for how much
 * of the buffer will be used in the worst case.  if it is 0, the hint is
 * discarded.  currently, the hint is ignored. */
void* do_alloc_conn_buffer(size_t max_rusage_hint) {
    conn_buffer_t* buffer;

    if ( (buffer = remove_conn_buffer_from_freelist(max_rusage_hint)) == NULL &&
         (buffer = make_conn_buffer()) == NULL ) {
        cbs.stats.allocs_failed ++;
        return NULL;
    }

    cbs.stats.allocs ++;

    assert(buffer->signature == CONN_BUFFER_SIGNATURE);
    assert(buffer->in_freelist == false);
    assert(buffer->used == false);
    buffer->used = true;
    buffer->rusage_updated = false;
    buffer->prev_rusage = buffer->max_rusage;

    do_conn_buffer_reclamation();

    return buffer->data;
}


void do_free_conn_buffer(void* ptr, ssize_t max_rusage) {
    conn_buffer_t* buffer = get_buffer_from_data_ptr(ptr);

    assert(buffer->signature == CONN_BUFFER_SIGNATURE);
    assert(buffer->in_freelist == false);
    assert(buffer->used == true);

    buffer->used = false;

    if (max_rusage == -1) {
        if (buffer->rusage_updated == false) {
            /* no one has reported any usage info on this block.  assume the worse. */
            max_rusage = CONN_BUFFER_SIZE;
        } else {
            max_rusage = buffer->max_rusage;
        }
    } else {
        max_rusage = max_rusage + CONN_BUFFER_HEADER_SZ;
    }
    max_rusage = round_up_to_page(max_rusage);

    if (buffer->max_rusage > max_rusage) {
        max_rusage = buffer->max_rusage;
    }

    // bump counter
    cbs.stats.frees ++;

    /* do we reclaim this buffer? */
    if (max_rusage >= cbs.settings.buffer_rsize_limit ||
        detect_corruption) {
        /* yes, reclaim now...  we must set the max_rusage to the previously
         * known rusage because that delta was never accounted for. */
        buffer->max_rusage = buffer->prev_rusage;
        destroy_conn_buffer(buffer);
    } else {
        /* adjust stats */
        cbs.total_rsize += (max_rusage - buffer->prev_rusage);

        /* return to the free list */
        add_conn_buffer_to_freelist(buffer);
    }

    /* should we start a reclamation? */
    if (cbs.reclamation_in_progress == false &&
        cbs.total_rsize >= cbs.settings.total_rsize_range_top) {
        cbs.stats.reclamations_started ++;
        cbs.reclamation_in_progress = true;
    }

    do_conn_buffer_reclamation();
}


void report_max_rusage(void* ptr, size_t max_rusage) {
    conn_buffer_t* buffer = get_buffer_from_data_ptr(ptr);

    assert(buffer->signature == CONN_BUFFER_SIGNATURE);
    assert(buffer->in_freelist == false);
    assert(buffer->used == true);

    buffer->rusage_updated = true;

    max_rusage = round_up_to_page(max_rusage + CONN_BUFFER_HEADER_SZ);
    if (max_rusage > buffer->max_rusage) {
        buffer->max_rusage = max_rusage;
    }

    /* yeah, we're reading a variable in a thread-unsafe way, but we'll do a
     * second check once we grab the lock. */
    if (cbs.reclamation_in_progress) {
        conn_buffer_reclamation();
    }
}


char* do_conn_buffer_stats(size_t* result_size) {
    size_t bufsize = 2048, offset = 0;
    char* buffer = malloc(bufsize);
    char terminator[] = "END\r\n";

    if (buffer == NULL) {
        *result_size = 0;
        return NULL;
    }

    offset = append_to_buffer(buffer, bufsize, offset, sizeof(terminator),
                              "STAT num_free_buffers %" PRINTF_INT64_MODIFIER "u\n"
                              "STAT total_rsize %" PRINTF_INT64_MODIFIER "u\n"
                              "STAT total_rsize_in_freelist %" PRINTF_INT64_MODIFIER "u\n"
                              "STAT allocates %" PRINTF_INT64_MODIFIER "u\n"
                              "STAT frees %" PRINTF_INT64_MODIFIER "u\n"
                              "STAT failed_allocates %" PRINTF_INT64_MODIFIER "u\n"
                              "STAT destroys %" PRINTF_INT64_MODIFIER "u\n"
                              "STAT reclamations_started %" PRINTF_INT64_MODIFIER "u\n",
                              cbs.num_free_buffers,
                              cbs.total_rsize,
                              cbs.total_rsize_in_freelist,
                              cbs.stats.allocs,
                              cbs.stats.frees,
                              cbs.stats.allocs_failed,
                              cbs.stats.destroys,
                              cbs.stats.reclamations_started);

    offset = append_to_buffer(buffer, bufsize, offset, 0, terminator);

    *result_size = offset;

    return buffer;
}
