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
static struct {
    size_t page_size;
    int global_initialized;
    conn_buffer_group_t* cbg_list;
    size_t cbg_count;
} l;


CB_STATIC int cb_freelist_check(conn_buffer_group_t* cbg) {
#if defined(FREELIST_CHECK)
    size_t i, found_entries, rsize_total;
    bool end_found = false;

    /* num free buffers agrees with reality? */
    for (i = 0, found_entries = 0, rsize_total = 0;
         i < cbg->free_buffer_list_capacity;
         i ++) {
        size_t left_child, right_child;

        if (cbg->free_buffers[i] == NULL) {
            end_found = true;
            continue;
        }

        assert(end_found == false);
        assert(cbg->free_buffers[i]->signature == CONN_BUFFER_SIGNATURE);
        assert(cbg->free_buffers[i]->in_freelist == true);
        assert(cbg->free_buffers[i]->used == false);
        found_entries ++;

        rsize_total += cbg->free_buffers[i]->max_rusage;

        left_child = HEAP_LEFT_CHILD(i);
        right_child = HEAP_RIGHT_CHILD(i);

        if (left_child < cbg->num_free_buffers) {
            /* valid left child */
            assert(cbg->free_buffers[left_child] != NULL);
            assert(cbg->free_buffers[left_child]->signature == CONN_BUFFER_SIGNATURE);
            assert(cbg->free_buffers[left_child]->in_freelist == true);
            assert(cbg->free_buffers[left_child]->used == false);

            assert(cbg->free_buffers[i]->max_rusage >= cbg->free_buffers[left_child]->max_rusage);
        }

        if (right_child < cbg->num_free_buffers) {
            /* valid right child */
            assert(cbg->free_buffers[right_child] != NULL);
            assert(cbg->free_buffers[right_child]->signature == CONN_BUFFER_SIGNATURE);
            assert(cbg->free_buffers[right_child]->in_freelist == true);
            assert(cbg->free_buffers[right_child]->used == false);

            assert(cbg->free_buffers[i]->max_rusage >= cbg->free_buffers[right_child]->max_rusage);
        }
    }

    assert(found_entries == cbg->num_free_buffers);
    assert(rsize_total == cbg->total_rsize_in_freelist);
#endif /* #if defined(FREELIST_CHECK) */

    return 0;
}


static size_t round_up_to_page(size_t bytes) {
    if ((bytes % l.page_size) != 0) {
        bytes = ((bytes + l.page_size - 1) & ~ (l.page_size - 1));
    }

    return bytes;
}


static void add_conn_buffer_to_freelist(conn_buffer_group_t* cbg, conn_buffer_t* buffer) {
    size_t index;

    assert(cb_freelist_check(cbg) == 0);
    (void) cb_freelist_check;      /* dummy rvalue to avoid compiler warning. */

    assert(buffer->signature == CONN_BUFFER_SIGNATURE);
    assert(buffer->in_freelist == false);
    assert(buffer->used == false);

    if (cbg->num_free_buffers >= cbg->free_buffer_list_capacity) {
        if (cbg->free_buffers == NULL) {
            cbg->free_buffer_list_capacity = cbg->settings.initial_buffer_count;
            cbg->free_buffers = (conn_buffer_t**) pool_malloc(sizeof(conn_buffer_t*) * cbg->free_buffer_list_capacity,
                                                             CONN_BUFFER_POOL);
        } else {
            cbg->free_buffers = pool_realloc(cbg->free_buffers,
                                            sizeof(conn_buffer_t*) * cbg->free_buffer_list_capacity * 2,
                                            sizeof(conn_buffer_t*) * cbg->free_buffer_list_capacity,
                                            CONN_BUFFER_POOL);
            cbg->free_buffer_list_capacity *= 2;
        }

        memset(&cbg->free_buffers[cbg->num_free_buffers], 0,
               sizeof(conn_buffer_t*) * (cbg->free_buffer_list_capacity - cbg->num_free_buffers));
    }

    buffer->in_freelist = true;

    assert(cbg->free_buffers[cbg->num_free_buffers] == NULL);
    cbg->free_buffers[cbg->num_free_buffers] = buffer;
    index = cbg->num_free_buffers;
    cbg->num_free_buffers ++;
    cbg->total_rsize_in_freelist += buffer->max_rusage;

    while (index != 0) {
        size_t parent_index = HEAP_PARENT(index);

        if (cbg->free_buffers[index]->max_rusage >
            cbg->free_buffers[parent_index]->max_rusage) {
            conn_buffer_t* temp;

            /* swap */
            temp = cbg->free_buffers[index];
            cbg->free_buffers[index] = cbg->free_buffers[parent_index];
            cbg->free_buffers[parent_index] = temp;
        } else {
            /* no swap occured, so we can stop the reheaping operation */
            break;
        }
    }
    assert(cb_freelist_check(cbg) == 0);
}


static conn_buffer_t* remove_conn_buffer_from_freelist(conn_buffer_group_t* cbg, size_t max_rusage_hint) {
    conn_buffer_t* ret;
    conn_buffer_t* compare;
    size_t index;

    assert(cb_freelist_check(cbg) == 0);

    if (cbg->num_free_buffers == 0) {
        assert(cbg->free_buffers[0] == NULL);
        return NULL;
    }

    ret = cbg->free_buffers[0];
    cbg->free_buffers[0] = NULL;
    assert(ret->signature == CONN_BUFFER_SIGNATURE);
    assert(ret->in_freelist == true);
    assert(ret->used == false);
    ret->in_freelist = false;

    cbg->num_free_buffers --;
    cbg->total_rsize_in_freelist -= ret->max_rusage;

    if (cbg->num_free_buffers == 0) {
        assert(cb_freelist_check(cbg) == 0);
        return ret;
    }

    index = 0;
    compare = cbg->free_buffers[cbg->num_free_buffers];
    cbg->free_buffers[cbg->num_free_buffers] = NULL;

    while (true) {
        size_t left_child_index = HEAP_LEFT_CHILD(index);
        size_t right_child_index = HEAP_RIGHT_CHILD(index);
        bool valid_left, valid_right, swap_left, swap_right;

        valid_left = (left_child_index < cbg->num_free_buffers) ? true : false;
        valid_right = (right_child_index < cbg->num_free_buffers) ? true : false;

        swap_left = (valid_left &&
                     cbg->free_buffers[left_child_index]->max_rusage >
                     compare->max_rusage) ? true : false;
        swap_right = (valid_right &&
                      cbg->free_buffers[right_child_index]->max_rusage >
                      compare->max_rusage) ? true : false;

        /* it's possible that we'd want to swap with both (i.e., bigger than
         * both).  pick the larger one to swap with.
         */
        if (swap_left && swap_right) {
            if (cbg->free_buffers[left_child_index]->max_rusage >
                cbg->free_buffers[right_child_index]->max_rusage) {
                /* left is greater, swap with left. */
                swap_right = false;
            } else {
                swap_left = false;
            }
        }

        if (swap_left) {
            assert(cbg->free_buffers[index] == NULL);
            cbg->free_buffers[index] = cbg->free_buffers[left_child_index];
            cbg->free_buffers[left_child_index] = NULL;
            index = left_child_index;
        } else if (swap_right) {
            assert(cbg->free_buffers[index] == NULL);
            cbg->free_buffers[index] = cbg->free_buffers[right_child_index];
            cbg->free_buffers[right_child_index] = NULL;
            index = right_child_index;
        } else {
            assert(cbg->free_buffers[index] == NULL);
            cbg->free_buffers[index] = compare;
            break;
        }
    }

    assert(cb_freelist_check(cbg) == 0);
    return ret;
}


static conn_buffer_t* make_conn_buffer(conn_buffer_group_t* cbg) {
    conn_buffer_t* buffer;

    if (cbg->total_rsize + l.page_size >= cbg->settings.total_rsize_range_top) {
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

    cbg->total_rsize += buffer->max_rusage;

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


static void destroy_conn_buffer(conn_buffer_group_t* cbg, conn_buffer_t* buffer) {
    void* ptr = buffer;
    size_t range = buffer->max_rusage;

    assert(buffer->in_freelist == false);
    assert(buffer->used == false);
    assert(cbg->total_rsize > 0);


    cbg->stats.destroys ++;
    cbg->total_rsize -= buffer->max_rusage;
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


static void conn_buffer_reclamation(conn_buffer_group_t* cbg) {
    if (cbg->reclamation_in_progress) {
        if (cbg->num_free_buffers != 0) {
            /* grab the most space-consuming buffer and reclaim it. */
            conn_buffer_t* tofree = remove_conn_buffer_from_freelist(cbg, CONN_BUFFER_SIZE);

            destroy_conn_buffer(cbg, tofree);
        }

        if (cbg->num_free_buffers == 0 ||
            cbg->total_rsize <= cbg->settings.total_rsize_range_bottom) {
            cbg->reclamation_in_progress = false;
        }
    }
}


static void conn_buffer_group_init(conn_buffer_group_t* const cbg,
                                   size_t initial_buffer_count,
                                   size_t buffer_rsize_limit,
                                   size_t total_rsize_range_bottom,
                                   size_t total_rsize_range_top) {
    size_t i;

    always_assert( cbg->initialized == false );
    always_assert( (CONN_BUFFER_HEADER_SZ % sizeof(void*)) == 0 );

    always_assert( (l.page_size & (l.page_size - 1)) == 0);

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

    always_assert(initial_buffer_count * l.page_size <= total_rsize_range_bottom);
    always_assert(initial_buffer_count * l.page_size <= total_rsize_range_top);
    // always_assert(buffer_rsize_limit < total_rsize_range_bottom);
    always_assert(total_rsize_range_bottom < total_rsize_range_top);
    always_assert(buffer_rsize_limit >= l.page_size);

    cbg->settings.initial_buffer_count = initial_buffer_count;
    cbg->settings.buffer_rsize_limit = buffer_rsize_limit;
    cbg->settings.total_rsize_range_bottom = total_rsize_range_bottom;
    cbg->settings.total_rsize_range_top = total_rsize_range_top;

    for (i = 0; i < initial_buffer_count; i ++) {
        conn_buffer_t* buffer;

        buffer = make_conn_buffer(cbg);
        always_assert(buffer != NULL);
        add_conn_buffer_to_freelist(cbg, buffer);
    }

    pthread_mutex_init(&cbg->lock, NULL);

    cbg->initialized = true;
}


void conn_buffer_init(unsigned groups,
                      size_t initial_buffer_count,
                      size_t buffer_rsize_limit,
                      size_t total_rsize_range_bottom,
                      size_t total_rsize_range_top) {
    unsigned ix;

    l.page_size = getpagesize();
    l.cbg_list = calloc(groups, sizeof(conn_buffer_group_t));

    for (ix = 0; ix < groups; ix ++) {
        conn_buffer_group_init(&l.cbg_list[ix], initial_buffer_count, buffer_rsize_limit,
                               total_rsize_range_bottom, total_rsize_range_top);
    }
    l.cbg_count = groups;

    l.global_initialized = true;
}


/**
 * allocate a connection buffer.  max_rusage_hint is a hint for how much
 * of the buffer will be used in the worst case.  if it is 0, the hint is
 * discarded.  currently, the hint is ignored.
 *
 * this is a thread-guarded function, i.e., it should only be called for a
 * connection buffer group by the thread it is assigned to.
 */
static void* do_alloc_conn_buffer(conn_buffer_group_t* cbg, size_t max_rusage_hint) {
    conn_buffer_t* buffer;

    assert(cbg->settings.tid == pthread_self());

    if ( (buffer = remove_conn_buffer_from_freelist(cbg, max_rusage_hint)) == NULL &&
         (buffer = make_conn_buffer(cbg)) == NULL ) {
        cbg->stats.allocs_failed ++;
        return NULL;
    }

    cbg->stats.allocs ++;

    assert(buffer->signature == CONN_BUFFER_SIGNATURE);
    assert(buffer->in_freelist == false);
    assert(buffer->used == false);
    buffer->used = true;
    buffer->rusage_updated = false;
    buffer->prev_rusage = buffer->max_rusage;

    conn_buffer_reclamation(cbg);

    return buffer->data;
}


/**
 * releases a connection buffer.  max_rusage_hint is a hint for how much of the
 * buffer was used in the worst case.  if it is 0 and no one has ever called
 * report_max_rusage on this buffer, it is assumed that the entire buffer has
 * been accessed.  if it is 0 and someone has called report_max_rusage, then the
 * largest value reported is used.
 *
 * this is a thread-guarded function, i.e., it should only be called for a
 * connection buffer group by the thread it is assigned to.
 */
static void do_free_conn_buffer(conn_buffer_group_t* cbg, void* ptr, ssize_t max_rusage) {
    conn_buffer_t* buffer = get_buffer_from_data_ptr(ptr);

    assert(cbg->settings.tid == pthread_self());
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
    cbg->stats.frees ++;

    /* do we reclaim this buffer? */
    if (max_rusage >= cbg->settings.buffer_rsize_limit ||
        detect_corruption) {
        /* yes, reclaim now...  we must set the max_rusage to the previously
         * known rusage because that delta was never accounted for. */
        buffer->max_rusage = buffer->prev_rusage;
        destroy_conn_buffer(cbg, buffer);
    } else {
        /* adjust stats */
        cbg->total_rsize += (max_rusage - buffer->prev_rusage);

        /* return to the free list */
        add_conn_buffer_to_freelist(cbg, buffer);
    }

    /* should we start a reclamation? */
    if (cbg->reclamation_in_progress == false &&
        cbg->total_rsize >= cbg->settings.total_rsize_range_top) {
        cbg->stats.reclamations_started ++;
        cbg->reclamation_in_progress = true;
    }

    conn_buffer_reclamation(cbg);
}


/**
 * report the maximum usage of a connection buffer.
 *
 * this is a thread-guarded function, i.e., it should only be called for a
 * connection buffer group by the thread it is assigned to.
 */
static void do_report_max_rusage(conn_buffer_group_t* cbg, void* ptr, size_t max_rusage) {
    conn_buffer_t* buffer = get_buffer_from_data_ptr(ptr);

    assert(cbg->settings.tid == pthread_self());
    assert(buffer->signature == CONN_BUFFER_SIGNATURE);
    assert(buffer->in_freelist == false);
    assert(buffer->used == true);

    buffer->rusage_updated = true;

    max_rusage = round_up_to_page(max_rusage + CONN_BUFFER_HEADER_SZ);
    if (max_rusage > buffer->max_rusage) {
        buffer->max_rusage = max_rusage;
    }

    /* yeah, we're reading a variable in a group-unsafe way, but we'll do a
     * second check once we grab the lock. */
    if (cbg->reclamation_in_progress) {
        conn_buffer_reclamation(cbg);
    }
}


void* alloc_conn_buffer(conn_buffer_group_t* cbg, size_t max_rusage_hint) {
    void* ret;

    pthread_mutex_lock(&cbg->lock);
    ret = do_alloc_conn_buffer(cbg, max_rusage_hint);
    pthread_mutex_unlock(&cbg->lock);
    return ret;
}

void free_conn_buffer(conn_buffer_group_t* cbg, void* ptr, ssize_t max_rusage) {
    pthread_mutex_lock(&cbg->lock);
    do_free_conn_buffer(cbg, ptr, max_rusage);
    pthread_mutex_unlock(&cbg->lock);
}

void report_max_rusage(conn_buffer_group_t* cbg, void* ptr, size_t max_rusage) {
    pthread_mutex_lock(&cbg->lock);
    do_report_max_rusage(cbg, ptr, max_rusage);
    pthread_mutex_unlock(&cbg->lock);
}


conn_buffer_group_t* get_conn_buffer_group(unsigned group) {
    assert(group < l.cbg_count);
    return &l.cbg_list[group];
}


/**
 * assign a thread id to a connection buffer group.  returns false if no errors
 * occur.
 */
bool assign_thread_id_to_conn_buffer_group(unsigned group, pthread_t tid) {
    assert(group < l.cbg_count);
    if (group < l.cbg_count) {
        assert(l.cbg_list[group].settings.tid == 0);
        if (l.cbg_list[group].settings.tid == 0) {
            l.cbg_list[group].settings.tid = tid;
            return false;
        }
    }
    return true;
}


char* conn_buffer_stats(size_t* result_size) {
    size_t bufsize = 2048, offset = 0;
    char* buffer = malloc(bufsize);
    char terminator[] = "END\r\n";
    unsigned ix;

    size_t num_free_buffers = 0;
    size_t total_rsize = 0;
    size_t total_rsize_in_freelist = 0;
    conn_buffer_stats_t stats;

    if (buffer == NULL) {
        *result_size = 0;
        return NULL;
    }

    memset(&stats, 0, sizeof(conn_buffer_stats_t));

    for (ix = 0; ix < l.cbg_count; ix ++) {
        pthread_mutex_lock(&l.cbg_list[ix].lock);
        num_free_buffers           += l.cbg_list[ix].num_free_buffers;
        total_rsize                += l.cbg_list[ix].total_rsize;
        total_rsize_in_freelist    += l.cbg_list[ix].total_rsize_in_freelist;
        stats.allocs               += l.cbg_list[ix].stats.allocs;
        stats.frees                += l.cbg_list[ix].stats.frees;
        stats.destroys             += l.cbg_list[ix].stats.destroys;
        stats.reclamations_started += l.cbg_list[ix].stats.reclamations_started;
        stats.allocs_failed        += l.cbg_list[ix].stats.allocs_failed;
        pthread_mutex_unlock(&l.cbg_list[ix].lock);
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
                              num_free_buffers,
                              total_rsize,
                              total_rsize_in_freelist,
                              stats.allocs,
                              stats.frees,
                              stats.allocs_failed,
                              stats.destroys,
                              stats.reclamations_started);

    offset = append_to_buffer(buffer, bufsize, offset, 0, terminator);

    *result_size = offset;

    return buffer;
}
