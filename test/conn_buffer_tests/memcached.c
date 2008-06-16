/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <assert.h>
#include <stdlib.h>

#include "conn_buffer.h"
#include "memcached.h"

struct stats_s stats;

/* should never be called. */
size_t append_to_buffer(char* const buffer_start,
                        const size_t buffer_size,
                        const size_t buffer_off,
                        const size_t reserved,
                        const char* fmt,
                        ...) {
    abort();
}


void conn_buffer_reset(void) {
    size_t i;

    assert(cb_freelist_check() == 0);
    assert(cbs.initialized == true);

    assert(cbs.stats.allocs == cbs.stats.frees);

    for (i = 0; i < cbs.num_free_buffers; i ++) {
        assert(cbs.free_buffers[i] != NULL);
        assert(cbs.free_buffers[i]->signature == CONN_BUFFER_SIGNATURE);
        assert(cbs.free_buffers[i]->in_freelist == true);
        assert(cbs.free_buffers[i]->used == false);
        assert(cbs.free_buffers[i]->max_rusage <= cbs.settings.buffer_rsize_limit);

        munmap(cbs.free_buffers[i], CONN_BUFFER_SIZE);
    }

    for ( ; i < cbs.free_buffer_list_capacity; i ++) {
        assert(cbs.free_buffers[i] == NULL);
    }

    free(cbs.free_buffers);

    cbs.initialized = false;
}
