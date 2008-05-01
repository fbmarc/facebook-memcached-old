/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "assoc.h"
#include "flat_storage.h"
#include "flat_storage_support.h"


/**
 * this is a series of tests that exercises the paged nature of the flat storage
 * allocator.
 */


/* test that unused_memory is set correcttly after init. */
static int
unused_memory_on_init_test(int verbose) {
    V_LPRINTF(1, "%s\n", __FUNCTION__);
    TASSERT(fsi.unused_memory == (TOTAL_MEMORY - FLAT_STORAGE_INCREMENT_DELTA));

    return 0;
}


/* iteratively allocate all available large chunks.  subsequently free all the
 * itmes.  ensure that the free lists are managed correctly. */
static int
alloc_all_large_chunks_test(int verbose) {
    size_t initial_freelist_sz = fsi.large_free_list_sz;
    item** item_list;
    size_t min_size_for_large_chunk = ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
        ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 1) * sizeof( ((small_body_chunk_t*) 0)->data ) ) +
        1;
    size_t counter;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    item_list = malloc(sizeof(item*) * initial_freelist_sz);

    for (counter = 0;
         counter < initial_freelist_sz;
         counter ++) {
        item* it;

        V_PRINTF(2, "\r  *  allocate chunk %lu", counter);
        V_FLUSH(2);

        it = do_item_alloc(NULL, 0,
                           FLAGS, current_time + 10000, min_size_for_large_chunk);
        TASSERT(it != NULL);
        TASSERT(is_item_large_chunk(it));
        TASSERT(chunks_in_item(it) == 1);

        TASSERT(fsi.large_free_list_sz == initial_freelist_sz - counter - 1);
        TASSERT(fsi.small_free_list_sz == 0);

        item_list[counter] = it;

        TASSERT(freelist_check(SMALL_CHUNK));
        TASSERT(freelist_check(LARGE_CHUNK));
        TASSERT(item_chunk_check(it));
    }

    V_PRINTF(2, "\n");

    TASSERT(fsi.large_free_list_sz == 0);
    TASSERT(fsi.unused_memory == (TOTAL_MEMORY - FLAT_STORAGE_INCREMENT_DELTA));

    for (counter = 0;
         counter < initial_freelist_sz;
         counter ++) {
        V_PRINTF(2, "\r  *  deallocate chunk %lu", counter);
        V_FLUSH(2);

        do_item_deref(item_list[counter]);
    }

    TASSERT(fsi.large_free_list_sz == initial_freelist_sz);
    TASSERT(fsi.small_free_list_sz == 0);
    TASSERT(freelist_check(SMALL_CHUNK));
    TASSERT(freelist_check(LARGE_CHUNK));
    TASSERT(fsi.unused_memory == (TOTAL_MEMORY - FLAT_STORAGE_INCREMENT_DELTA));

    V_PRINTF(2, "\n");

    return 0;
}


/* iteratively allocate all available large chunks.  then allocate one extra
 * large chunk.  this should force the paging system to grab more memory.
 * subsequently free all the itmes.  ensure that the free lists are managed
 * correctly. */
static int
bump_paging_limit_test(int verbose) {
    size_t initial_freelist_sz = fsi.large_free_list_sz;
    item** item_list;
    size_t min_size_for_large_chunk = ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
        ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 1) * sizeof( ((small_body_chunk_t*) 0)->data ) ) +
        1;
    size_t counter;
    item* it;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    item_list = malloc(sizeof(item*) * (initial_freelist_sz + 1));

    for (counter = 0;
         counter < initial_freelist_sz;
         counter ++) {

        V_PRINTF(2, "\r  *  allocate chunk %lu", counter);
        V_FLUSH(2);

        it = do_item_alloc(NULL, 0,
                           FLAGS, current_time + 10000, min_size_for_large_chunk);
        TASSERT(it != NULL);
        TASSERT(is_item_large_chunk(it));
        TASSERT(chunks_in_item(it) == 1);

        TASSERT(fsi.large_free_list_sz == initial_freelist_sz - counter - 1);
        TASSERT(fsi.small_free_list_sz == 0);

        item_list[counter] = it;

        TASSERT(freelist_check(SMALL_CHUNK));
        TASSERT(freelist_check(LARGE_CHUNK));
        TASSERT(item_chunk_check(it));
    }

    V_PRINTF(2, "\n");

    V_PRINTF(2, "\r  *  allocate extra chunk");

    TASSERT(fsi.large_free_list_sz == 0);
    TASSERT(fsi.unused_memory == (TOTAL_MEMORY - FLAT_STORAGE_INCREMENT_DELTA));

    it = do_item_alloc(NULL, 0,
                       FLAGS, current_time + 10000, min_size_for_large_chunk);
    TASSERT(it != NULL);
    TASSERT(is_item_large_chunk(it));
    TASSERT(chunks_in_item(it) == 1);

    TASSERT(fsi.large_free_list_sz == initial_freelist_sz - 1);
    TASSERT(fsi.small_free_list_sz == 0);

    item_list[counter] = it;

    for (counter = 0;
         counter < initial_freelist_sz + 1;
         counter ++) {
        V_PRINTF(2, "\r  *  deallocate chunk %lu", counter);
        V_FLUSH(2);

        do_item_deref(item_list[counter]);
    }

    TASSERT(fsi.large_free_list_sz == initial_freelist_sz * 2);
    TASSERT(fsi.small_free_list_sz == 0);
    TASSERT(freelist_check(SMALL_CHUNK));
    TASSERT(freelist_check(LARGE_CHUNK));
    TASSERT(fsi.unused_memory == (TOTAL_MEMORY - (FLAT_STORAGE_INCREMENT_DELTA * 2)));

    V_PRINTF(2, "\n");

    return 0;
}


/* iteratively allocate all memory available to the system..  then allocate one
 * extra large chunk.  this should fail.  subsequently free all the itmes.
 * ensure that the free lists are managed correctly. */
static int
bump_total_limit_test(int verbose) {
    item** item_list;
    size_t min_size_for_large_chunk = ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
        ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 1) * sizeof( ((small_body_chunk_t*) 0)->data ) ) +
        1;
    size_t counter;
    item* it;
    size_t items;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    items = (TOTAL_MEMORY + LARGE_CHUNK_SZ - 1) / LARGE_CHUNK_SZ;
    item_list = malloc( sizeof(item*) * items );

    for (counter = 0;
         counter < items;
         counter ++) {

        V_PRINTF(2, "\r  *  allocate chunk %lu", counter);
        V_FLUSH(2);

        it = do_item_alloc(NULL, 0,
                           FLAGS, current_time + 10000, min_size_for_large_chunk);
        TASSERT(it != NULL);
        TASSERT(is_item_large_chunk(it));
        TASSERT(chunks_in_item(it) == 1);

        TASSERT(fsi.small_free_list_sz == 0);

        item_list[counter] = it;

        TASSERT(freelist_check(SMALL_CHUNK));
        TASSERT(freelist_check(LARGE_CHUNK));
        TASSERT(item_chunk_check(it));
    }

    V_PRINTF(2, "\n");

    TASSERT(fsi.large_free_list_sz == 0);
    TASSERT(fsi.unused_memory == 0);

    V_PRINTF(2, "\r  *  allocate extra chunk");

    it = do_item_alloc(NULL, 0,
                       FLAGS, current_time + 10000, min_size_for_large_chunk);
    TASSERT(it == NULL);

    TASSERT(fsi.large_free_list_sz == 0);
    TASSERT(fsi.unused_memory == 0);

    for (counter = 0;
         counter < items;
         counter ++) {
        V_PRINTF(2, "\r  *  deallocate chunk %lu", counter);
        V_FLUSH(2);

        do_item_deref(item_list[counter]);
    }

    TASSERT(fsi.large_free_list_sz == items);
    TASSERT(fsi.small_free_list_sz == 0);
    TASSERT(freelist_check(SMALL_CHUNK));
    TASSERT(freelist_check(LARGE_CHUNK));
    TASSERT(fsi.unused_memory == 0);

    V_PRINTF(2, "\n");

    return 0;
}


tester_info_t tests[] = {
    {unused_memory_on_init_test, 1},
    {alloc_all_large_chunks_test, 1},
    {bump_paging_limit_test, 1},
    {bump_total_limit_test, 1},
};


#include "main.h"
