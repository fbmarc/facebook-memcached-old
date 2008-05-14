/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "assoc.h"
#include "flat_storage.h"
#include "flat_storage_support.h"


struct in_addr addr = { INADDR_NONE };


/**
 * this is a set of complex tests that require assoc to have been tested, yet
 * are really part of alloc tests.
 */


/*
 * allocate nearly all memory with small items (all memory -
 * SMALL_CHUNKS_PER_LARGE_CHUNK - 1).  then free one item such that we now have
 * SMALL_CHUNKS_PER_LARGE_CHUNK free small chunks, but they are not contiguous
 * and cannot be coalesced.  then allocate one large object.  this will require
 * the migration of single chunk items.  this covers strategy 3 of do_item_alloc
 * for large items.
 */
static int
migrate_small_single_chunk_item_test(int verbose) {
    typedef struct {
        item* it;
        char key[KEY_MAX_LENGTH];
        uint8_t klen;
    } test_keys_t;

    size_t num_objects = fsi.large_free_list_sz * SMALL_CHUNKS_PER_LARGE_CHUNK;
    test_keys_t* items = malloc(sizeof(test_keys_t) * num_objects);
    item* lru_trigger;
    size_t max_small_key_size = SMALL_TITLE_CHUNK_DATA_SZ;
    size_t min_size_for_large_chunk = ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
        ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 1) * sizeof( ((small_body_chunk_t*) 0)->data ) ) +
        1;
    size_t i, count;
    char key[KEY_MAX_LENGTH];
    size_t klen;
    size_t large_free_list_sz = fsi.large_free_list_sz, small_free_list_sz = fsi.small_free_list_sz;

    V_PRINTF(1, "  * %s\n", __FUNCTION__);

    TASSERT(fsi.large_free_list_sz != 0);
    TASSERT(fsi.small_free_list_sz == 0);

    for (i = 0, count = 0;
         fsi.large_free_list_sz ||
             fsi.small_free_list_sz > SMALL_CHUNKS_PER_LARGE_CHUNK - 1;
         i ++, count ++) {
        V_PRINTF(2, "\r  *  allocating small object %lu", i);
        V_FLUSH(2);

        assert(i < num_objects);

        do {
            items[i].klen = make_random_key(items[i].key, max_small_key_size);
        } while (assoc_find(items[i].key, items[i].klen));

        items[i].it = do_item_alloc(items[i].key, items[i].klen,
                                    FLAGS, 0, 0, addr);
        TASSERT(items[i].it);
        TASSERT(is_item_large_chunk(items[i].it) == 0);

        do_item_link(items[i].it);
    }
    V_PRINTF(2, "\n");

    TASSERT(fsi.large_free_list_sz == 0);
    TASSERT(fsi.small_free_list_sz == SMALL_CHUNKS_PER_LARGE_CHUNK - 1);

    /* release the first item we allocated.  we should now have
     * SMALL_CHUNKS_PER_LARGE_CHUNK free small chunks.  but since they don't
     * have the same parent block, they can't be coalesced. */
    do_item_unlink(items[0].it, UNLINK_NORMAL);
    do_item_deref(items[0].it);

    /* dereference the last item we allocated.  otherwise nothing is eligible to
     * move. */
    do_item_deref(items[count - 1].it);

    TASSERT(fsi.large_free_list_sz == 0);
    TASSERT(fsi.small_free_list_sz == SMALL_CHUNKS_PER_LARGE_CHUNK);

    V_LPRINTF(2, "alloc\n");
    do {
        klen = make_random_key(key, max_small_key_size);
    } while (assoc_find(key, klen));

    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, min_size_for_large_chunk - klen,
                                addr);
    TASSERT(lru_trigger != NULL);

    V_LPRINTF(2, "ensuring that objects that shouldn't be evicted are still present\n");
    for (i = 1; i < count; i ++) {
        TASSERT((items[i].it = assoc_find(items[i].key, items[i].klen)));
    }

    V_LPRINTF(2, "cleanup objects\n");
    for (i = 1; i < count; i ++) {
        do_item_unlink(items[i].it, UNLINK_NORMAL);
    }
    for (i = 1; i < count - 1; i ++) {
        do_item_deref(items[i].it);
    }
    do_item_deref(lru_trigger);

    TASSERT(fsi.large_free_list_sz == large_free_list_sz &&
            fsi.small_free_list_sz == small_free_list_sz);

    return 0;
}


tester_info_t tests[] = {
  {migrate_small_single_chunk_item_test, 1},
};


#include "main.h"
