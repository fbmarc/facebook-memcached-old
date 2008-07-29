/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "assoc.h"
#include "flat_storage.h"
#include "flat_storage_support.h"


struct in_addr addr = { INADDR_NONE };


/**
 * this is a series of tests that exercises the eviction from LRU when we
 * allocate a small item.
 */


/* allocate all memory with small chunks.  allocate one more object.  it should
 * free up the oldest object.  release all objects.  this covers case 1 for the
 * small item alloc in flat_storage_lru_evict(..). */
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
            small_items[i].klen = make_random_key(small_items[i].key, max_key_size, true);
        } while (assoc_find(small_items[i].key, small_items[i].klen));

        small_items[i].it = do_item_alloc(small_items[i].key, small_items[i].klen, FLAGS, 0, 0,
                                          addr);
        TASSERT(small_items[i].it);
        TASSERT(is_item_large_chunk(small_items[i].it) == false);

        do_item_link(small_items[i].it, small_items[i].key);
    }
    V_PRINTF(2, "\n");

    TASSERT(fsi.large_free_list_sz == 0 &&
            fsi.small_free_list_sz == 0);

    V_LPRINTF(2, "alloc before deref\n");
    do {
        klen = make_random_key(key, max_key_size, true);
    } while (assoc_find(key, klen));

    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, 0, addr);
    TASSERT(lru_trigger == NULL);

    V_LPRINTF(2, "dereferencing objects\n");
    for (i = 0; i < num_objects; i ++) {
        do_item_deref(small_items[i].it);
    }

    V_LPRINTF(2, "alloc after deref\n");
    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, 0, addr);
    TASSERT(lru_trigger != NULL);

    V_LPRINTF(2, "search for evicted object\n");
    TASSERT(assoc_find(small_items[0].key, small_items[0].klen) == NULL);

    V_LPRINTF(2, "ensuring that objects that shouldn't be evicted are still present\n");
    for (i = 1; i < num_objects; i ++) {
        TASSERT(assoc_find(small_items[i].key, small_items[i].klen));
    }

    V_LPRINTF(2, "cleanup objects\n");
    for (i = 1; i < num_objects; i ++) {
        do_item_unlink(small_items[i].it, UNLINK_NORMAL, small_items[i].key);
    }
    do_item_deref(lru_trigger);

    TASSERT(fsi.large_free_list_sz == large_free_list_sz &&
            fsi.small_free_list_sz == small_free_list_sz);

    return 0;
}


/* allocate all memory with large chunks.  allocate one small object.  it should
 * free up the oldest object.  release all objects.  this covers case 2 for the
 * small item alloc in flat_storage_lru_evict(..). */
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
    size_t i;
    size_t min_size_for_large_chunk = ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
        ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 1) * sizeof( ((small_body_chunk_t*) 0)->data ) ) +
        1;
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
            large_items[i].klen = make_random_key(large_items[i].key, max_key_size, true);
        } while (assoc_find(large_items[i].key, large_items[i].klen));

        large_items[i].it = do_item_alloc(large_items[i].key, large_items[i].klen,
                                          FLAGS, 0,
                                          min_size_for_large_chunk - large_items[i].klen,
                                          addr);
        TASSERT(large_items[i].it);
        TASSERT(is_item_large_chunk(large_items[i].it));

        do_item_link(large_items[i].it, large_items[i].key);
    }
    V_PRINTF(2, "\n");

    TASSERT(fsi.large_free_list_sz == 0 &&
            fsi.small_free_list_sz == 0);

    V_LPRINTF(2, "alloc before deref\n");
    do {
        klen = make_random_key(key, max_key_size, true);
    } while (assoc_find(key, klen));

    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, 0, addr);
    TASSERT(lru_trigger == NULL);

    V_LPRINTF(2, "dereferencing objects\n");
    for (i = 0; i < num_objects; i ++) {
        do_item_deref(large_items[i].it);
    }

    V_LPRINTF(2, "alloc after deref\n");
    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, 0, addr);
    TASSERT(lru_trigger != NULL);

    V_LPRINTF(2, "search for evicted object\n");
    TASSERT(assoc_find(large_items[0].key, large_items[0].klen) == NULL);

    V_LPRINTF(2, "ensuring that objects that shouldn't be evicted are still present\n");
    for (i = 1; i < num_objects; i ++) {
        TASSERT(assoc_find(large_items[i].key, large_items[i].klen));
    }

    V_LPRINTF(2, "cleanup objects\n");
    for (i = 1; i < num_objects; i ++) {
        do_item_unlink(large_items[i].it, UNLINK_NORMAL, large_items[i].key);
    }
    do_item_deref(lru_trigger);

    TASSERT(fsi.large_free_list_sz == large_free_list_sz &&
            fsi.small_free_list_sz == small_free_list_sz);

    return 0;
}


/*
 * allocate all memory with small and large chunks.  link them such that the
 * small items are the oldest.  allocate one small object that can be covered by
 * the release of one small item.  this covers part of case 3 for the small item
 * alloc in flat_storage_lru_evict(..).
 */
static int
mixed_items_release_one_small_item_test(int verbose) {
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
            small_items[i].klen = make_random_key(small_items[i].key, max_small_key_size, true);
        } while (assoc_find(small_items[i].key, small_items[i].klen));

        small_items[i].it = do_item_alloc(small_items[i].key, small_items[i].klen,
                                          FLAGS, 0, 0,
                                          addr);
        TASSERT(small_items[i].it);
        TASSERT(is_item_large_chunk(small_items[i].it) == 0);

        do_item_link(small_items[i].it, small_items[i].key);
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
            large_items[i].klen = make_random_key(large_items[i].key, KEY_MAX_LENGTH, true);
        } while (assoc_find(large_items[i].key, large_items[i].klen));

        large_items[i].it = do_item_alloc(large_items[i].key, large_items[i].klen,
                                          FLAGS, 0,
                                          min_size_for_large_chunk - large_items[i].klen,
                                          addr);
        TASSERT(large_items[i].it);
        TASSERT(is_item_large_chunk(large_items[i].it));

        do_item_link(large_items[i].it, large_items[i].key);
    }
    V_PRINTF(2, "\n");

    TASSERT(fsi.large_free_list_sz == 0 &&
            fsi.small_free_list_sz == 0);

    V_LPRINTF(2, "alloc before deref\n");
    do {
        klen = make_random_key(key, max_small_key_size, true);
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
    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, 0, addr);
    TASSERT(lru_trigger != NULL);

    V_LPRINTF(2, "search for evicted object\n");
    TASSERT(assoc_find(small_items[0].key, small_items[0].klen) == NULL);

    V_LPRINTF(2, "ensuring that objects that shouldn't be evicted are still present\n");
    for (i = 1; i < num_small_objects; i ++) {
        TASSERT(assoc_find(small_items[i].key, small_items[i].klen));
    }
    for (i = 0; i < num_large_objects; i ++) {
        TASSERT(assoc_find(large_items[i].key, large_items[i].klen));
    }

    V_LPRINTF(2, "cleanup objects\n");
    for (i = 1; i < num_small_objects; i ++) {
        do_item_unlink(small_items[i].it, UNLINK_NORMAL, small_items[i].key);
    }
    for (i = 0; i < num_large_objects; i ++) {
        do_item_unlink(large_items[i].it, UNLINK_NORMAL, large_items[i].key);
    }
    do_item_deref(lru_trigger);

    TASSERT(fsi.large_free_list_sz == large_free_list_sz &&
            fsi.small_free_list_sz == small_free_list_sz);

    return 0;
}


/*
 * allocate all memory with small and large chunks.  link them such that the
 * small items are the oldest.  allocate one small object that can be covered by
 * the release of one large item.  this covers part of case 4 for the small item
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
            large_items[i].klen = make_random_key(large_items[i].key, KEY_MAX_LENGTH, true);
        } while (assoc_find(large_items[i].key, large_items[i].klen));

        large_items[i].it = do_item_alloc(large_items[i].key, large_items[i].klen,
                                          FLAGS, 0,
                                          min_size_for_large_chunk - large_items[i].klen,
            addr);
        TASSERT(large_items[i].it);
        TASSERT(is_item_large_chunk(large_items[i].it));

        do_item_link(large_items[i].it, large_items[i].key);
    }
    V_PRINTF(2, "\n");

    for (i = 0; i < num_small_objects; i ++) {
        V_PRINTF(2, "\r  *  allocating small object %lu", i);
        V_FLUSH(2);
        do {
            small_items[i].klen = make_random_key(small_items[i].key, max_small_key_size, true);
        } while (assoc_find(small_items[i].key, small_items[i].klen));

        small_items[i].it = do_item_alloc(small_items[i].key, small_items[i].klen,
                                          FLAGS, 0, 0,
                                          addr);
        TASSERT(small_items[i].it);
        TASSERT(is_item_large_chunk(small_items[i].it) == 0);

        do_item_link(small_items[i].it, small_items[i].key);
    }
    V_PRINTF(2, "\n");

    TASSERT(fsi.large_free_list_sz == 0 &&
            fsi.small_free_list_sz == 0);

    V_LPRINTF(2, "alloc before deref\n");
    do {
        klen = make_random_key(key, max_small_key_size, true);
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
    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, 0, addr);
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
        do_item_unlink(small_items[i].it, UNLINK_NORMAL, small_items[i].key);
    }
    for (i = 1; i < num_large_objects; i ++) {
        do_item_unlink(large_items[i].it, UNLINK_NORMAL, large_items[i].key);
    }
    do_item_deref(lru_trigger);

    TASSERT(fsi.large_free_list_sz == large_free_list_sz &&
            fsi.small_free_list_sz == small_free_list_sz);

    return 0;
}


/*
 * allocate all memory with small and large chunks.  link them such that the
 * small items are the oldest.  allocate one small object that can be covered by
 * the release of one small item and one large item.  this covers part of case 3
 * and part of case 4 for the small item alloc in flat_storage_lru_evict(..).
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
            small_items[i].klen = make_random_key(small_items[i].key, max_small_key_size, true);
        } while (assoc_find(small_items[i].key, small_items[i].klen));

        small_items[i].it = do_item_alloc(small_items[i].key, small_items[i].klen,
                                          FLAGS, 0,
                                          0, addr);
        TASSERT(small_items[i].it);
        TASSERT(is_item_large_chunk(small_items[i].it) == 0);

        do_item_link(small_items[i].it, small_items[i].key);
    }
    V_PRINTF(2, "\n");

    for (i = 0; i < num_large_objects; i ++) {
        V_PRINTF(2, "\r  *  allocating large object %lu", i);
        V_FLUSH(2);
        do {
            large_items[i].klen = make_random_key(large_items[i].key, KEY_MAX_LENGTH, true);
        } while (assoc_find(large_items[i].key, large_items[i].klen));

        large_items[i].it = do_item_alloc(large_items[i].key, large_items[i].klen,
                                          FLAGS, 0,
                                          min_size_for_large_chunk - large_items[i].klen, addr);
        TASSERT(large_items[i].it);
        TASSERT(is_item_large_chunk(large_items[i].it));

        do_item_link(large_items[i].it, large_items[i].key);
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
        klen = make_random_key(key, max_small_key_size, true);
    } while (assoc_find(key, klen));
    lru_trigger = do_item_alloc(key, klen, FLAGS, 0, SMALL_TITLE_CHUNK_DATA_SZ - klen + 1, addr);
    TASSERT(lru_trigger != NULL);
    TASSERT(is_item_large_chunk(lru_trigger) == 0);
    TASSERT(chunks_in_item(lru_trigger) > 1);

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
        do_item_unlink(small_items[i].it, UNLINK_NORMAL, small_items[i].key);
    }
    for (i = 1; i < num_large_objects; i ++) {
        do_item_unlink(large_items[i].it, UNLINK_NORMAL, large_items[i].key);
    }
    do_item_deref(lru_trigger);

    TASSERT(fsi.large_free_list_sz == large_free_list_sz &&
            fsi.small_free_list_sz == small_free_list_sz);

    return 0;
}


tester_info_t tests[] = {
    {all_small_chunks_test, 1},
    {all_large_chunks_test, 1},
    {mixed_items_release_one_small_item_test, 1},
    {mixed_items_release_one_large_item_test, 1},
    {mixed_items_release_small_and_large_items_test, 1},
};


#include "main.h"

