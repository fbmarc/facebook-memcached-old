/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "memcached.h"
#include "flat_storage.h"
#include "flat_storage_support.h"


static int
initialized_test(int verbose) {
    V_LPRINTF(1, "%s\n", __FUNCTION__);

    return (!fsi.initialized);
}


static int
flat_storage_increment_delta_test(int verbose) {
    intptr_t start = (intptr_t) fsi.flat_storage_start;
    intptr_t end   = (intptr_t) fsi.uninitialized_start;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    /* if FLAT_STORAGE_INCREMENT_DELTA from flat_storage.c changes, this test
     * will fail.
     */
    TASSERT((end - start + sizeof(large_chunk_t) - 1) / sizeof(large_chunk_t) ==
            (LARGE_CHUNK_SZ * 1024 + sizeof(large_chunk_t) - 1) / sizeof(large_chunk_t));

    TASSERT(fsi.large_free_list_sz ==
            (LARGE_CHUNK_SZ * 1024 + sizeof(large_chunk_t) - 1) / sizeof(large_chunk_t));

    return 0;
}


/* this verifies that the flags are set correctly.
 */
static int
flags_test(int verbose) {
    /* grab a large chunk indirectly. */
    large_chunk_t* lc;
    int i;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    for (i = 0, lc = fsi.flat_storage_start;
         lc < fsi.uninitialized_start;
         i ++, lc ++) {
        TASSERT(lc->flags == (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_FREE));
    }

    return 0;
}


/* ensures that the free list contains the correct number of items.  count the
 * items and ensure that there are no duplicates */
static int
free_list_test(int verbose) {
    /* grab a large chunk indirectly */
    large_chunk_t* lc, * lc_freelist_walk;
    int i;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    /* walk through the free list, and ensure that the elements in the free list
     * are set up properly. */
    V_LPRINTF(2, "free list walk test\n");
    for (i = 0, lc_freelist_walk = fsi.large_free_list;
         lc_freelist_walk != NULL;
         i ++, lc_freelist_walk = lc_freelist_walk->lc_free.next) {
        TASSERT(lc_freelist_walk->flags == (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_FREE));
    }

    V_LPRINTF(2, "free list walk test summary\n");
    TASSERT(fsi.large_free_list_sz == i);

    /* walk through all the locations that should be free are in the free list. */
    for (lc = fsi.flat_storage_start;
         lc < fsi.uninitialized_start;
         lc ++) {
        for (lc_freelist_walk = fsi.large_free_list;
             lc_freelist_walk != NULL;
             lc_freelist_walk = lc_freelist_walk->lc_free.next) {
            if (lc == lc_freelist_walk) {
                break;
            }
        }

        TASSERT(lc_freelist_walk != NULL);
    }

    return 0;
}


/* at the point of initialization, no small chunks should be allocated. */
static int
small_chunk_test(int verbose) {
    V_LPRINTF(1, "%s\n", __FUNCTION__);

    TASSERT(fsi.small_free_list == NULL_CHUNKPTR);

    TASSERT(fsi.small_free_list_sz == 0);

    return 0;
}


static int
lru_test(int verbose) {
    V_LPRINTF(1, "%s\n", __FUNCTION__);

    TASSERT(fsi.large_lru_head == NULL &&
            fsi.large_lru_tail == NULL &&
            fsi.small_lru_head == NULL &&
            fsi.small_lru_tail == NULL);

    return 0;
}


tester_info_t tests[] = { {initialized_test, 1},
                          {flat_storage_increment_delta_test, 1},
                          {flags_test, 1},
                          {free_list_test, 1},
                          {small_chunk_test, 1},
                          {lru_test, 1},
};


#include "main.h"
