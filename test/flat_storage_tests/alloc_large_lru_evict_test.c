/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "assoc.h"
#include "flat_storage.h"
#include "flat_storage_support.h"


struct in_addr addr = { INADDR_NONE };


/**
 * this is a series of tests that exercises the eviction from LRU when we
 * allocate a large item.
 */


/* allocate all memory with small chunks.  allocate one more object.  it should
 * free up SMALL_CHUNKS_PER_LARGE_CHUNK small items.  release all objects.  this
 * covers part of case 1 for the large item alloc in
 * flat_storage_lru_evict(..). */
static int
all_small_chunks_test(int verbose) {
    typedef struct {
        item* it;
        char key[KEY_MAX_LENGTH];
        uint8_t klen;
    } all_small_chunks_key_t;

    size_t num_objects = fsi.large_free_list_sz * SMALL_CHUNKS_PER_LARGE_CHUNK;
    all_small_chunks_key_t* small_items = malloc(sizeof(all_small_chunks_key_t) * num_objects);
    item* lru_trigger;
    size_t max_key_size = SMALL_TITLE_CHUNK_DATA_SZ;
    size_t min_size_for_large_chunk = ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
        ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 1) * sizeof( ((small_body_chunk_t*) 0)->data ) ) +
        1;
    size_t i;
    char key[KEY_MAX_LENGTH];
    size_t klen;
    size_t large_free_list_sz = fsi.large_free_list_sz, small_free_list_sz = fsi.small_free_list_sz;

    V_PRINTF(1, "  * %s\n", __FUNCTION__);

    TASSERT(fsi.large_free_list_sz != 0);
    TASSERT(fsi.small_free_list_sz == 0);

    for (i = 0; i < num_objects; i ++) {
        V_PRINTF(2, "\r  *  allocating object %lu", i);
        V_FLUSH(2);
        do {
            small_items[i].klen = make_random_key(small_items[i].key, max_key_size);
        } while (assoc_find(small_items[i].key, small_items[i].klen));

        small_items[i].it = do_item_alloc(small_items[i].key, small_items[i].klen, FLAGS, 0, 0, addr);
        TASSERT(small_items[i].it);
        TASSERT(is_item_large_chunk(small_items[i].it) == false);

        do_item_link(small_items[i].it);
    }
    V_PRINTF(2, "\n");

    TASSERT(fsi.large_free_list_sz == 0 &&
            fsi.small_free_list_sz == 0);

    V_LPRINTF(2, "alloc before deref\n");
    do {
        klen = make_random_key(key, max_key_size);
    } while (assoc_find(key, klen));

    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, min_size_for_large_chunk - klen, addr);
    TASSERT(lru_trigger == NULL);

    V_LPRINTF(2, "dereferencing objects\n");
    for (i = 0; i < num_objects; i ++) {
        do_item_deref(small_items[i].it);
    }

    V_LPRINTF(2, "alloc after deref\n");
    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, min_size_for_large_chunk - klen, addr);
    TASSERT(lru_trigger != NULL);

    V_LPRINTF(2, "search for evicted object\n");
    for (i = 0; i < SMALL_CHUNKS_PER_LARGE_CHUNK; i ++) {
        TASSERT(assoc_find(small_items[i].key, small_items[i].klen) == NULL);
    }

    V_LPRINTF(2, "ensuring that objects that shouldn't be evicted are still present\n");
    for (i = SMALL_CHUNKS_PER_LARGE_CHUNK; i < num_objects; i ++) {
        TASSERT(assoc_find(small_items[i].key, small_items[i].klen));
    }

    V_LPRINTF(2, "cleanup objects\n");
    for (i = SMALL_CHUNKS_PER_LARGE_CHUNK; i < num_objects; i ++) {
        do_item_unlink(small_items[i].it, UNLINK_NORMAL);
    }
    do_item_deref(lru_trigger);

    TASSERT(fsi.large_free_list_sz == large_free_list_sz &&
            fsi.small_free_list_sz == small_free_list_sz);

    return 0;
}


/* allocate all memory with large chunks.  allocate one large object.  it should
 * free up the oldest object.  release all objects.  this covers case 2 for the
 * large item alloc in flat_storage_lru_evict(..). */
static int
all_large_chunks_test(int verbose) {
    typedef struct {
        item* it;
        char key[KEY_MAX_LENGTH];
        uint8_t klen;
    } all_small_chunks_key_t;

    size_t num_objects = fsi.large_free_list_sz;
    all_small_chunks_key_t* large_items = malloc(sizeof(all_small_chunks_key_t) * num_objects);
    item* lru_trigger;
    size_t max_key_size = SMALL_TITLE_CHUNK_DATA_SZ;
    size_t min_size_for_large_chunk = ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
        ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 1) * sizeof( ((small_body_chunk_t*) 0)->data ) ) +
        1;
    size_t i;
    char key[KEY_MAX_LENGTH];
    size_t klen;
    size_t large_free_list_sz = fsi.large_free_list_sz, small_free_list_sz = fsi.small_free_list_sz;

    V_PRINTF(1, "  * %s\n", __FUNCTION__);

    TASSERT(fsi.large_free_list_sz != 0);
    TASSERT(fsi.small_free_list_sz == 0);

    for (i = 0; i < num_objects; i ++) {
        V_PRINTF(2, "\r  *  allocating object %lu", i);
        V_FLUSH(2);
        do {
            large_items[i].klen = make_random_key(large_items[i].key, max_key_size);
        } while (assoc_find(large_items[i].key, large_items[i].klen));

        large_items[i].it = do_item_alloc(large_items[i].key, large_items[i].klen,
                                          FLAGS, 0,
                                          min_size_for_large_chunk - large_items[i].klen,
                                          addr);
        TASSERT(large_items[i].it);
        TASSERT(is_item_large_chunk(large_items[i].it));

        do_item_link(large_items[i].it);
    }
    V_PRINTF(2, "\n");

    TASSERT(fsi.large_free_list_sz == 0 &&
            fsi.small_free_list_sz == 0);

    V_LPRINTF(2, "alloc before deref\n");
    do {
        klen = make_random_key(key, max_key_size);
    } while (assoc_find(key, klen));

    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, min_size_for_large_chunk - klen,
                                addr);
    TASSERT(lru_trigger == NULL);

    V_LPRINTF(2, "dereferencing objects\n");
    for (i = 0; i < num_objects; i ++) {
        do_item_deref(large_items[i].it);
    }

    V_LPRINTF(2, "alloc after deref\n");
    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, min_size_for_large_chunk - klen, addr);
    TASSERT(lru_trigger != NULL);

    V_LPRINTF(2, "search for evicted object\n");
    TASSERT(assoc_find(large_items[0].key, large_items[0].klen) == NULL);

    V_LPRINTF(2, "ensuring that objects that shouldn't be evicted are still present\n");
    for (i = 1; i < num_objects; i ++) {
        TASSERT(assoc_find(large_items[i].key, large_items[i].klen));
    }

    V_LPRINTF(2, "cleanup objects\n");
    for (i = 1; i < num_objects; i ++) {
        do_item_unlink(large_items[i].it, UNLINK_NORMAL);
    }
    do_item_deref(lru_trigger);

    TASSERT(fsi.large_free_list_sz == large_free_list_sz &&
            fsi.small_free_list_sz == small_free_list_sz);

    return 0;
}


/*
 * allocate all memory with small items.  allocate one large object that can be
 * covered by the release of small items, but also requires the migration of
 * single chunk items.  this covers part of case 1 for the large item alloc in
 * flat_storage_lru_evict(..).
 */
static int
all_small_items_migrate_small_single_chunk_items_test(int verbose) {
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
    size_t i;
    char key[KEY_MAX_LENGTH];
    size_t klen;
    size_t large_free_list_sz = fsi.large_free_list_sz, small_free_list_sz = fsi.small_free_list_sz;

    V_PRINTF(1, "  * %s\n", __FUNCTION__);

    TASSERT(fsi.large_free_list_sz != 0);
    TASSERT(fsi.small_free_list_sz == 0);

    for (i = 0; i < num_objects; i ++) {
        V_PRINTF(2, "\r  *  allocating small object %lu", i);
        V_FLUSH(2);
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

    TASSERT(fsi.large_free_list_sz == 0 &&
            fsi.small_free_list_sz == 0);

    /* access items we don't want to move. */
    current_time += ITEM_UPDATE_INTERVAL + 1;
    /* touch every other item.  the ones that are not touched in (0,
     * SMALL_CHUNKS_PER_LARGE_CHUNK * 2) will be evicted. */
    for (i = 0; i < SMALL_CHUNKS_PER_LARGE_CHUNK * 2; i += 2) {
        do_item_update(items[i].it);
    }
    /* touch remaining items */
    for (i = SMALL_CHUNKS_PER_LARGE_CHUNK * 2; i < num_objects; i ++) {
        do_item_update(items[i].it);
    }

    V_LPRINTF(2, "dereferencing objects\n");
    for (i = 0; i < num_objects; i ++) {
        do_item_deref(items[i].it);
    }

    V_LPRINTF(2, "alloc after deref\n");
    do {
        klen = make_random_key(key, max_small_key_size);
    } while (assoc_find(key, klen));

    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, min_size_for_large_chunk - klen, addr);
    TASSERT(lru_trigger != NULL);

    V_LPRINTF(2, "search for evicted object\n");
    for (i = 1; i < SMALL_CHUNKS_PER_LARGE_CHUNK * 2; i += 2) {
        TASSERT(assoc_find(items[i].key, items[i].klen) == NULL);
    }

    V_LPRINTF(2, "ensuring that objects that shouldn't be evicted are still present\n");
    for (i = 0; i < SMALL_CHUNKS_PER_LARGE_CHUNK * 2; i += 2) {
        /* these may have been moved. */
        TASSERT((items[i].it = assoc_find(items[i].key, items[i].klen)));
    }
    for (i = SMALL_CHUNKS_PER_LARGE_CHUNK * 2; i < num_objects; i ++) {
        TASSERT(assoc_find(items[i].key, items[i].klen));
    }

    V_LPRINTF(2, "cleanup objects\n");
    for (i = 0; i < SMALL_CHUNKS_PER_LARGE_CHUNK * 2; i += 2) {
        do_item_unlink(items[i].it, UNLINK_NORMAL);
    }
    for (i = SMALL_CHUNKS_PER_LARGE_CHUNK * 2; i < num_objects; i ++) {
        do_item_unlink(items[i].it, UNLINK_NORMAL);
    }
    do_item_deref(lru_trigger);

    TASSERT(fsi.large_free_list_sz == large_free_list_sz &&
            fsi.small_free_list_sz == small_free_list_sz);

    return 0;
}


/*
 * allocate nearly all memory with small items (all memory -
 * SMALL_CHUNKS_PER_LARGE_CHUNK - 1).  then set it up such that there is only
 * one item eligible to be freed (i.e., by removing the remaining items from the
 * LRU.  allocate one large object.  this will require the migration of one
 * single chunk item at the LRU head.  this covers part of case 1 for the small
 * item alloc in flat_storage_lru_evict(..).
 */
static int
all_small_items_migrate_small_single_chunk_item_at_lru_head_test(int verbose) {
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

    // remove all but one item from the LRU.  and release our reference to the
    // item we don't remove from the LRU.
    for (i = 0; i < count - 1; i ++) {
        do_item_unlink(items[i].it, UNLINK_NORMAL);
    }
    do_item_deref(items[count - 1].it);

    TASSERT(fsi.lru_head == items[count - 1].it);

    TASSERT(fsi.large_free_list_sz == 0);
    TASSERT(fsi.small_free_list_sz == SMALL_CHUNKS_PER_LARGE_CHUNK - 1);

    V_LPRINTF(2, "alloc\n");
    do {
        klen = make_random_key(key, max_small_key_size);
    } while (assoc_find(key, klen));

    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, min_size_for_large_chunk - klen, addr);
    TASSERT(lru_trigger != NULL);

    V_LPRINTF(2, "search for evicted object\n");
    TASSERT(assoc_find(items[count - 1].key, items[count - 1].klen) == NULL);

    V_LPRINTF(2, "cleanup objects\n");
    for (i = 0; i < count - 1; i ++) {
        do_item_deref(items[i].it);
    }
    do_item_deref(lru_trigger);

    TASSERT(fsi.large_free_list_sz == large_free_list_sz &&
            fsi.small_free_list_sz == small_free_list_sz);

    return 0;
}


/*
 * this is a negative test to ensure the proper behavior when we don't have
 * sufficient resources.  in this case, we have sufficient small items on the
 * LRU, and enough of them have refcount == 0, but all the parent broken chunks
 * have refcount > 0.
 */
static int
insufficient_available_large_broken_chunks(int verbose) {
    typedef struct {
        item* it;
        char key[KEY_MAX_LENGTH];
        uint8_t klen;
    } all_small_chunks_key_t;

    size_t num_objects = fsi.large_free_list_sz * SMALL_CHUNKS_PER_LARGE_CHUNK;
    all_small_chunks_key_t* small_items = malloc(sizeof(all_small_chunks_key_t) * num_objects);
    item* lru_trigger;
    size_t max_key_size = SMALL_TITLE_CHUNK_DATA_SZ;
    size_t min_size_for_large_chunk = ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
        ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 1) * sizeof( ((small_body_chunk_t*) 0)->data ) ) +
        1;
    size_t i;
    char key[KEY_MAX_LENGTH];
    size_t klen;
    size_t large_free_list_sz = fsi.large_free_list_sz, small_free_list_sz = fsi.small_free_list_sz;

    V_PRINTF(1, "  * %s\n", __FUNCTION__);

    TASSERT(fsi.large_free_list_sz != 0);
    TASSERT(fsi.small_free_list_sz == 0);

    for (i = 0; i < num_objects; i ++) {
        V_PRINTF(2, "\r  *  allocating object %lu", i);
        V_FLUSH(2);
        do {
            small_items[i].klen = make_random_key(small_items[i].key, max_key_size);
        } while (assoc_find(small_items[i].key, small_items[i].klen));

        small_items[i].it = do_item_alloc(small_items[i].key, small_items[i].klen, FLAGS, 0, 0, addr);
        TASSERT(small_items[i].it);
        TASSERT(is_item_large_chunk(small_items[i].it) == false);

        do_item_link(small_items[i].it);
    }
    V_PRINTF(2, "\n");

    TASSERT(fsi.large_free_list_sz == 0 &&
            fsi.small_free_list_sz == 0);

    V_LPRINTF(2, "alloc before deref\n");
    do {
        klen = make_random_key(key, max_key_size);
    } while (assoc_find(key, klen));

    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, min_size_for_large_chunk - klen, addr);
    TASSERT(lru_trigger == NULL);

    V_LPRINTF(2, "dereferencing objects\n");
    for (i = 0; i < num_objects; i += 2) {
        do_item_deref(small_items[i].it);
    }

    V_LPRINTF(2, "alloc after deref\n");
    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, min_size_for_large_chunk - klen, addr);
    TASSERT(lru_trigger == NULL);

    V_LPRINTF(2, "ensuring that objects that shouldn't be evicted are still present\n");
    for (i = 0; i < num_objects; i ++) {
        bool should_be_found;
        /* we free everything we encounter that has no refcount until we hit the
         * LRU_SEARCH_DEPTH, at which time we cease searching. */
        if (i % 2 == 0 && i < (LRU_SEARCH_DEPTH * 2)) {
            should_be_found = false;
        } else {
            should_be_found = true;
        }
        TASSERT((assoc_find(small_items[i].key, small_items[i].klen) ? (true) : (false)) ==
                should_be_found);
    }

    V_LPRINTF(2, "cleanup objects\n");
    for (i = 0; i < num_objects; i ++) {
        /* we dereference all the odd numbered items */
        if ((i % 2) != 0) {
            do_item_deref(small_items[i].it);
        }

        /* we unlink everything that's still in the LRU. */
        if (i % 2 == 0 && i < (LRU_SEARCH_DEPTH * 2)) {
            ;
        } else {
            do_item_unlink(small_items[i].it, UNLINK_NORMAL);
        }
    }

    TASSERT(fsi.large_free_list_sz == large_free_list_sz &&
            fsi.small_free_list_sz == small_free_list_sz);

    return 0;
}


/*
 * allocate all memory with small and large chunks.  link them such that the
 * small items are the oldest.  allocate one large object that can be covered by
 * the release of small items, but does not require any migration.  this covers
 * part of case 3 for the large item alloc in flat_storage_lru_evict(..).
 */
static int
mixed_items_release_small_items_test(int verbose) {
    typedef struct {
        item* it;
        char key[KEY_MAX_LENGTH];
        uint8_t klen;
    } mixed_items_release_one_small_item_t;

    size_t num_small_objects = (fsi.large_free_list_sz / 2) * SMALL_CHUNKS_PER_LARGE_CHUNK;
    /* this is not the same as fsi.large_free_list_sz / 2 due to rounding. */
    size_t num_large_objects = fsi.large_free_list_sz - (fsi.large_free_list_sz / 2);
    mixed_items_release_one_small_item_t* large_items = malloc(sizeof(mixed_items_release_one_small_item_t) *
                                                               num_large_objects);
    mixed_items_release_one_small_item_t* small_items = malloc(sizeof(mixed_items_release_one_small_item_t) *
                                                               num_small_objects);
    item* lru_trigger;
    size_t max_small_key_size = SMALL_TITLE_CHUNK_DATA_SZ;
    size_t min_size_for_large_chunk = ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
        ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 1) * sizeof( ((small_body_chunk_t*) 0)->data ) ) +
        1;
    size_t i;
    char key[KEY_MAX_LENGTH];
    size_t klen;
    size_t large_free_list_sz = fsi.large_free_list_sz, small_free_list_sz = fsi.small_free_list_sz;

    V_PRINTF(1, "  * %s\n", __FUNCTION__);

    TASSERT(fsi.large_free_list_sz != 0);
    TASSERT(fsi.small_free_list_sz == 0);

    for (i = 0; i < num_small_objects; i ++) {
        V_PRINTF(2, "\r  *  allocating small object %lu", i);
        V_FLUSH(2);
        do {
            small_items[i].klen = make_random_key(small_items[i].key, max_small_key_size);
        } while (assoc_find(small_items[i].key, small_items[i].klen));

        small_items[i].it = do_item_alloc(small_items[i].key, small_items[i].klen,
                                          FLAGS, 0,
                                          0, addr);
        TASSERT(small_items[i].it);
        TASSERT(is_item_large_chunk(small_items[i].it) == 0);

        do_item_link(small_items[i].it);
    }
    V_PRINTF(2, "\n");

    /*
     * in case of a tie, the large item is the one evicted.  to ensure that the
     * small item is evicted, it needs to have an older timestamp.
     */
    current_time += 1;

    for (i = 0; i < num_large_objects; i ++) {
        V_PRINTF(2, "\r  *  allocating large object %lu", i);
        V_FLUSH(2);
        do {
            large_items[i].klen = make_random_key(large_items[i].key, KEY_MAX_LENGTH);
        } while (assoc_find(large_items[i].key, large_items[i].klen));

        large_items[i].it = do_item_alloc(large_items[i].key, large_items[i].klen,
                                          FLAGS, 0,
                                          min_size_for_large_chunk - large_items[i].klen,
                                          addr);
        TASSERT(large_items[i].it);
        TASSERT(is_item_large_chunk(large_items[i].it));

        do_item_link(large_items[i].it);
    }
    V_PRINTF(2, "\n");

    TASSERT(fsi.large_free_list_sz == 0 &&
            fsi.small_free_list_sz == 0);

    V_LPRINTF(2, "alloc before deref\n");
    do {
        klen = make_random_key(key, max_small_key_size);
    } while (assoc_find(key, klen));

    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, min_size_for_large_chunk - klen, addr);
    TASSERT(lru_trigger == NULL);

    V_LPRINTF(2, "dereferencing objects\n");
    for (i = 0; i < num_small_objects; i ++) {
        do_item_deref(small_items[i].it);
    }
    for (i = 0; i < num_large_objects; i ++) {
        do_item_deref(large_items[i].it);
    }

    V_LPRINTF(2, "alloc after deref\n");
    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, min_size_for_large_chunk - klen, addr);
    TASSERT(lru_trigger != NULL);

    V_LPRINTF(2, "search for evicted object\n");
    for (i = 0; i < SMALL_CHUNKS_PER_LARGE_CHUNK; i ++) {
        TASSERT(assoc_find(small_items[i].key, small_items[i].klen) == NULL);
    }

    V_LPRINTF(2, "ensuring that objects that shouldn't be evicted are still present\n");
    for (i = SMALL_CHUNKS_PER_LARGE_CHUNK; i < num_small_objects; i ++) {
        TASSERT(assoc_find(small_items[i].key, small_items[i].klen));
    }
    for (i = 0; i < num_large_objects; i ++) {
        TASSERT(assoc_find(large_items[i].key, large_items[i].klen));
    }

    V_LPRINTF(2, "cleanup objects\n");
    for (i = SMALL_CHUNKS_PER_LARGE_CHUNK; i < num_small_objects; i ++) {
        do_item_unlink(small_items[i].it, UNLINK_NORMAL);
    }
    for (i = 0; i < num_large_objects; i ++) {
        do_item_unlink(large_items[i].it, UNLINK_NORMAL);
    }
    do_item_deref(lru_trigger);

    TASSERT(fsi.large_free_list_sz == large_free_list_sz &&
            fsi.small_free_list_sz == small_free_list_sz);

    return 0;
}


/*
 * allocate all memory with small and large chunks.  link them such that the
 * small items are the oldest.  allocate one large object that can be covered by
 * the release of one large item.  this covers part of case 4 for the large item
 * alloc in flat_storage_lru_evict(..).
 */
static int
mixed_items_release_one_large_item_test(int verbose) {
    typedef struct {
        item* it;
        char key[KEY_MAX_LENGTH];
        uint8_t klen;
    } mixed_items_release_one_small_item_t;

    size_t num_small_objects = (fsi.large_free_list_sz / 2) * SMALL_CHUNKS_PER_LARGE_CHUNK;
    /* this is not the same as fsi.large_free_list_sz / 2 due to rounding. */
    size_t num_large_objects = fsi.large_free_list_sz - (fsi.large_free_list_sz / 2);
    mixed_items_release_one_small_item_t* large_items = malloc(sizeof(mixed_items_release_one_small_item_t) *
                                                               num_large_objects);
    mixed_items_release_one_small_item_t* small_items = malloc(sizeof(mixed_items_release_one_small_item_t) *
                                                               num_small_objects);
    item* lru_trigger;
    size_t max_small_key_size = SMALL_TITLE_CHUNK_DATA_SZ;
    size_t min_size_for_large_chunk = ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
        ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 1) * sizeof( ((small_body_chunk_t*) 0)->data ) ) +
        1;
    size_t i;
    char key[KEY_MAX_LENGTH];
    size_t klen;
    size_t large_free_list_sz = fsi.large_free_list_sz, small_free_list_sz = fsi.small_free_list_sz;

    V_PRINTF(1, "  * %s\n", __FUNCTION__);

    TASSERT(fsi.large_free_list_sz != 0);
    TASSERT(fsi.small_free_list_sz == 0);

    for (i = 0; i < num_large_objects; i ++) {
        V_PRINTF(2, "\r  *  allocating large object %lu", i);
        V_FLUSH(2);
        do {
            large_items[i].klen = make_random_key(large_items[i].key, KEY_MAX_LENGTH);
        } while (assoc_find(large_items[i].key, large_items[i].klen));

        large_items[i].it = do_item_alloc(large_items[i].key, large_items[i].klen,
                                          FLAGS, 0,
                                          min_size_for_large_chunk - large_items[i].klen,
                                          addr);
        TASSERT(large_items[i].it);
        TASSERT(is_item_large_chunk(large_items[i].it));

        do_item_link(large_items[i].it);
    }
    V_PRINTF(2, "\n");

    for (i = 0; i < num_small_objects; i ++) {
        V_PRINTF(2, "\r  *  allocating small object %lu", i);
        V_FLUSH(2);
        do {
            small_items[i].klen = make_random_key(small_items[i].key, max_small_key_size);
        } while (assoc_find(small_items[i].key, small_items[i].klen));

        small_items[i].it = do_item_alloc(small_items[i].key, small_items[i].klen,
                                          FLAGS, 0,
                                          0, addr);
        TASSERT(small_items[i].it);
        TASSERT(is_item_large_chunk(small_items[i].it) == 0);

        do_item_link(small_items[i].it);
    }
    V_PRINTF(2, "\n");

    TASSERT(fsi.large_free_list_sz == 0 &&
            fsi.small_free_list_sz == 0);

    V_LPRINTF(2, "alloc before deref\n");
    do {
        klen = make_random_key(key, max_small_key_size);
    } while (assoc_find(key, klen));

    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, 0, addr);
    TASSERT(lru_trigger == NULL);

    V_LPRINTF(2, "dereferencing objects\n");
    for (i = 0; i < num_small_objects; i ++) {
        do_item_deref(small_items[i].it);
    }
    for (i = 0; i < num_large_objects; i ++) {
        do_item_deref(large_items[i].it);
    }

    V_LPRINTF(2, "alloc after deref\n");
    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, min_size_for_large_chunk - klen, addr);
    TASSERT(lru_trigger != NULL);

    V_LPRINTF(2, "search for evicted object\n");
    TASSERT(assoc_find(large_items[0].key, large_items[0].klen) == NULL);

    V_LPRINTF(2, "ensuring that objects that shouldn't be evicted are still present\n");
    for (i = 0; i < num_small_objects; i ++) {
        TASSERT(assoc_find(small_items[i].key, small_items[i].klen));
    }
    for (i = 1; i < num_large_objects; i ++) {
        TASSERT(assoc_find(large_items[i].key, large_items[i].klen));
    }

    V_LPRINTF(2, "cleanup objects\n");
    for (i = 0; i < num_small_objects; i ++) {
        do_item_unlink(small_items[i].it, UNLINK_NORMAL);
    }
    for (i = 1; i < num_large_objects; i ++) {
        do_item_unlink(large_items[i].it, UNLINK_NORMAL);
    }
    do_item_deref(lru_trigger);

    TASSERT(fsi.large_free_list_sz == large_free_list_sz &&
            fsi.small_free_list_sz == small_free_list_sz);

    return 0;
}


/*
 * allocate all memory with small and large chunks.  link them such the
 * allocation of a large object that spans two chunks will evict
 * SMALL_CHUNKS_PER_LARGE_CHUNK small chunks and one large chunk.  this covers
 * part of case 3 and part of case 4 for the small item alloc in
 * flat_storage_lru_evict(..).
 */
static int
mixed_items_release_small_and_large_items_test(int verbose) {
    typedef struct {
        item* it;
        char key[KEY_MAX_LENGTH];
        uint8_t klen;
    } mixed_items_release_one_small_item_t;

    size_t num_small_objects = (fsi.large_free_list_sz / 2) * SMALL_CHUNKS_PER_LARGE_CHUNK;
    /* this is not the same as fsi.large_free_list_sz / 2 due to rounding. */
    size_t num_large_objects = fsi.large_free_list_sz - (fsi.large_free_list_sz / 2);
    mixed_items_release_one_small_item_t* large_items = malloc(sizeof(mixed_items_release_one_small_item_t) *
                                                               num_large_objects);
    mixed_items_release_one_small_item_t* small_items = malloc(sizeof(mixed_items_release_one_small_item_t) *
                                                               num_small_objects);
    item* lru_trigger;
    size_t max_small_key_size = SMALL_TITLE_CHUNK_DATA_SZ;
    size_t min_size_for_large_chunk = ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
        ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 1) * sizeof( ((small_body_chunk_t*) 0)->data ) ) +
        1;
    size_t i;
    char key[KEY_MAX_LENGTH];
    size_t klen;
    size_t large_free_list_sz = fsi.large_free_list_sz, small_free_list_sz = fsi.small_free_list_sz;

    V_PRINTF(1, "  * %s\n", __FUNCTION__);

    TASSERT(fsi.large_free_list_sz != 0);
    TASSERT(fsi.small_free_list_sz == 0);

    for (i = 0; i < num_small_objects; i ++) {
        V_PRINTF(2, "\r  *  allocating small object %lu", i);
        V_FLUSH(2);
        do {
            small_items[i].klen = make_random_key(small_items[i].key, max_small_key_size);
        } while (assoc_find(small_items[i].key, small_items[i].klen));

        small_items[i].it = do_item_alloc(small_items[i].key, small_items[i].klen,
                                          FLAGS, 0,
                                          0, addr);
        TASSERT(small_items[i].it);
        TASSERT(is_item_large_chunk(small_items[i].it) == 0);

        do_item_link(small_items[i].it);
    }
    V_PRINTF(2, "\n");

    for (i = 0; i < num_large_objects; i ++) {
        V_PRINTF(2, "\r  *  allocating large object %lu", i);
        V_FLUSH(2);
        do {
            large_items[i].klen = make_random_key(large_items[i].key, KEY_MAX_LENGTH);
        } while (assoc_find(large_items[i].key, large_items[i].klen));

        large_items[i].it = do_item_alloc(large_items[i].key, large_items[i].klen,
                                          FLAGS, 0,
                                          min_size_for_large_chunk - large_items[i].klen, addr);
        TASSERT(large_items[i].it);
        TASSERT(is_item_large_chunk(large_items[i].it));

        do_item_link(large_items[i].it);
    }
    V_PRINTF(2, "\n");

    TASSERT(fsi.large_free_list_sz == 0 &&
            fsi.small_free_list_sz == 0);

    V_LPRINTF(2, "update items\n");
    /* update the objects we want to clobber *first*.  but since ties go to the
     * large item, we need to bump the time stamp to ensure the small item is
     * released first. */
    current_time += ITEM_UPDATE_INTERVAL + 1; /* initial bump to ensure that
                                               * LRU reordering takes place. */

    for (i = 0; i < SMALL_CHUNKS_PER_LARGE_CHUNK; i ++) {
        do_item_update(small_items[i].it);
    }
    current_time += 1;
    do_item_update(large_items[0].it);

    /* bump the timestamp and add the remaining items. */
    current_time += 1;
    for (i = SMALL_CHUNKS_PER_LARGE_CHUNK; i < num_small_objects; i ++) {
        do_item_update(small_items[i].it);
    }
    for (i = 1; i < num_large_objects; i ++) {
        do_item_update(large_items[i].it);
    }

    V_LPRINTF(2, "dereferencing objects\n");
    for (i = 0; i < num_small_objects; i ++) {
        do_item_deref(small_items[i].it);
    }
    for (i = 0; i < num_large_objects; i ++) {
        do_item_deref(large_items[i].it);
    }

    V_LPRINTF(2, "alloc after deref\n");
    do {
        klen = make_random_key(key, max_small_key_size);
    } while (assoc_find(key, klen));
    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, LARGE_TITLE_CHUNK_DATA_SZ - klen + 1, addr);
    TASSERT(lru_trigger != NULL);
    TASSERT(is_item_large_chunk(lru_trigger));
    TASSERT(chunks_in_item(lru_trigger) > 1);

    V_LPRINTF(2, "search for evicted objects\n");
    for (i = 0; i < SMALL_CHUNKS_PER_LARGE_CHUNK; i ++) {
        TASSERT(assoc_find(small_items[i].key, small_items[i].klen) == NULL);
    }
    TASSERT(assoc_find(large_items[0].key, large_items[0].klen) == NULL);

    V_LPRINTF(2, "ensuring that objects that shouldn't be evicted are still present\n");
    for (i = SMALL_CHUNKS_PER_LARGE_CHUNK; i < num_small_objects; i ++) {
        TASSERT(assoc_find(small_items[i].key, small_items[i].klen));
    }
    for (i = 1; i < num_large_objects; i ++) {
        TASSERT(assoc_find(large_items[i].key, large_items[i].klen));
    }

    V_LPRINTF(2, "cleanup objects\n");
    for (i = SMALL_CHUNKS_PER_LARGE_CHUNK; i < num_small_objects; i ++) {
        do_item_unlink(small_items[i].it, UNLINK_NORMAL);
    }
    for (i = 1; i < num_large_objects; i ++) {
        do_item_unlink(large_items[i].it, UNLINK_NORMAL);
    }
    do_item_deref(lru_trigger);

    TASSERT(fsi.large_free_list_sz == large_free_list_sz &&
            fsi.small_free_list_sz == small_free_list_sz);

    return 0;
}


/*
 * allocate all memory with small and large chunks.  link them such the
 * allocation of a large object will start evicting small chunks but then stop
 * because the large chunk LRU has an older item.  this covers part of case 3
 * and part of case 4 for the small item alloc in flat_storage_lru_evict(..).
 */
static int
mixed_items_release_small_and_large_items_scan_stop_test(int verbose) {
    typedef struct {
        item* it;
        char key[KEY_MAX_LENGTH];
        uint8_t klen;
    } mixed_items_release_one_small_item_t;

    size_t num_small_objects = (fsi.large_free_list_sz / 2) * SMALL_CHUNKS_PER_LARGE_CHUNK;
    /* this is not the same as fsi.large_free_list_sz / 2 due to rounding. */
    size_t num_large_objects = fsi.large_free_list_sz - (fsi.large_free_list_sz / 2);
    mixed_items_release_one_small_item_t* large_items = malloc(sizeof(mixed_items_release_one_small_item_t) *
                                                               num_large_objects);
    mixed_items_release_one_small_item_t* small_items = malloc(sizeof(mixed_items_release_one_small_item_t) *
                                                               num_small_objects);
    item* lru_trigger;
    size_t max_small_key_size = SMALL_TITLE_CHUNK_DATA_SZ;
    size_t min_size_for_large_chunk = ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
        ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 1) * sizeof( ((small_body_chunk_t*) 0)->data ) ) +
        1;
    size_t i;
    char key[KEY_MAX_LENGTH];
    size_t klen;
    size_t large_free_list_sz = fsi.large_free_list_sz, small_free_list_sz = fsi.small_free_list_sz;

    V_PRINTF(1, "  * %s\n", __FUNCTION__);

    TASSERT(fsi.large_free_list_sz != 0);
    TASSERT(fsi.small_free_list_sz == 0);

    for (i = 0; i < num_small_objects; i ++) {
        V_PRINTF(2, "\r  *  allocating small object %lu", i);
        V_FLUSH(2);
        do {
            small_items[i].klen = make_random_key(small_items[i].key, max_small_key_size);
        } while (assoc_find(small_items[i].key, small_items[i].klen));

        small_items[i].it = do_item_alloc(small_items[i].key, small_items[i].klen,
                                          FLAGS, 0,
                                          0, addr);
        TASSERT(small_items[i].it);
        TASSERT(is_item_large_chunk(small_items[i].it) == 0);

        do_item_link(small_items[i].it);
    }
    V_PRINTF(2, "\n");

    for (i = 0; i < num_large_objects; i ++) {
        V_PRINTF(2, "\r  *  allocating large object %lu", i);
        V_FLUSH(2);
        do {
            large_items[i].klen = make_random_key(large_items[i].key, KEY_MAX_LENGTH);
        } while (assoc_find(large_items[i].key, large_items[i].klen));

        large_items[i].it = do_item_alloc(large_items[i].key, large_items[i].klen,
                                          FLAGS, 0,
                                          min_size_for_large_chunk - large_items[i].klen, addr);
        TASSERT(large_items[i].it);
        TASSERT(is_item_large_chunk(large_items[i].it));

        do_item_link(large_items[i].it);
    }
    V_PRINTF(2, "\n");

    TASSERT(fsi.large_free_list_sz == 0 &&
            fsi.small_free_list_sz == 0);

    V_LPRINTF(2, "update items\n");
    /* update the objects we want to clobber *first*.  but since ties go to the
     * large item, we need to bump the time stamp to ensure the small item is
     * released first. */
    current_time += ITEM_UPDATE_INTERVAL + 1; /* initial bump to ensure that
                                               * LRU reordering takes place. */

    do_item_update(small_items[0].it);
    current_time += 1;
    do_item_update(large_items[0].it);

    /* bump the timestamp and add the remaining items. */
    current_time += 1;
    for (i = 1; i < num_small_objects; i ++) {
        do_item_update(small_items[i].it);
    }
    for (i = 1; i < num_large_objects; i ++) {
        do_item_update(large_items[i].it);
    }

    V_LPRINTF(2, "dereferencing objects\n");
    for (i = 0; i < num_small_objects; i ++) {
        do_item_deref(small_items[i].it);
    }
    for (i = 0; i < num_large_objects; i ++) {
        do_item_deref(large_items[i].it);
    }

    V_LPRINTF(2, "alloc after deref\n");
    do {
        klen = make_random_key(key, max_small_key_size);
    } while (assoc_find(key, klen));
    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, LARGE_TITLE_CHUNK_DATA_SZ - klen, addr);
    TASSERT(lru_trigger != NULL);
    TASSERT(is_item_large_chunk(lru_trigger));

    V_LPRINTF(2, "search for evicted objects\n");
    TASSERT(assoc_find(small_items[0].key, small_items[0].klen) == NULL);
    TASSERT(assoc_find(large_items[0].key, large_items[0].klen) == NULL);

    V_LPRINTF(2, "ensuring that objects that shouldn't be evicted are still present\n");
    for (i = 1; i < num_small_objects; i ++) {
        TASSERT(assoc_find(small_items[i].key, small_items[i].klen));
    }
    for (i = 1; i < num_large_objects; i ++) {
        TASSERT(assoc_find(large_items[i].key, large_items[i].klen));
    }

    V_LPRINTF(2, "cleanup objects\n");
    for (i = 1; i < num_small_objects; i ++) {
        do_item_unlink(small_items[i].it, UNLINK_NORMAL);
    }
    for (i = 1; i < num_large_objects; i ++) {
        do_item_unlink(large_items[i].it, UNLINK_NORMAL);
    }
    do_item_deref(lru_trigger);

    TASSERT(fsi.large_free_list_sz == large_free_list_sz &&
            fsi.small_free_list_sz == small_free_list_sz);

    return 0;
}


tester_info_t tests[] = {
    {all_small_chunks_test, 1},
    {all_large_chunks_test, 1},
    {all_small_items_migrate_small_single_chunk_items_test, 1},
    {all_small_items_migrate_small_single_chunk_item_at_lru_head_test, 1},
    {insufficient_available_large_broken_chunks, 1},
    {mixed_items_release_small_items_test, 1},
    {mixed_items_release_one_large_item_test, 1},
    {mixed_items_release_small_and_large_items_test, 1},
    {mixed_items_release_small_and_large_items_scan_stop_test, 1},
};


#include "main.h"
