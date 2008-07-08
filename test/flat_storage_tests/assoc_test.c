/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "assoc.h"
#include "flat_storage.h"
#include "flat_storage_support.h"


struct in_addr addr = { INADDR_NONE };


/**
 * this is a series of tests that exercise the assoc.c stub.  in the process,
 * this tests the item_link and item_unlink api in flat_storage.c.
 */


/* Allocate an item, link it.  Then try finding it.  Unlink the object and
 * remove our reference.  Test refcounts, chunk flags, and free list.  then
 * repeat with a small item. */
static int
link_unlink_test(int verbose) {
    size_t freelist_sz = fsi.large_free_list_sz;
    size_t min_size_for_large_chunk = ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
        ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 1) * sizeof( ((small_body_chunk_t*) 0)->data ) ) +
        1;
    item* it, * it_find;
    chunk_t* chunk;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    /**
     * large
     */
    V_LPRINTF(2, "large\n");
    V_LPRINTF(3, "allocate\n");
    it = do_item_alloc(KEY, sizeof(KEY) - sizeof(""),
                       FLAGS, current_time + 10000, min_size_for_large_chunk,
                       addr);
    TASSERT(it != NULL);
    TASSERT(is_item_large_chunk(it));
    TASSERT((it->empty_header.it_flags & ITEM_LINKED) == 0);
    TASSERT(it->empty_header.refcount == 1);

    chunk = (chunk_t*) it;
    TASSERT(&chunk->lc.lc_title == &it->large_title);

    V_LPRINTF(3, "link\n");
    do_item_link(it);
    TASSERT((it->empty_header.it_flags & ITEM_LINKED) != 0);
    TASSERT(it->empty_header.refcount == 1);
    TASSERT(lru_check());

    V_LPRINTF(3, "find\n");
    it_find = assoc_find(KEY, sizeof(KEY) - sizeof(""));
    TASSERT(it_find == it);

    V_LPRINTF(3, "unlink\n");
    do_item_unlink(it, UNLINK_NORMAL);
    TASSERT((it->empty_header.it_flags & ITEM_LINKED) == 0);
    TASSERT(it->empty_header.refcount == 1);
    TASSERT(lru_check());

    /* make sure we didn't free it while the refcount == 1 */
    TASSERT(chunk->lc.flags == (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_USED | LARGE_CHUNK_TITLE));

    V_LPRINTF(3, "find\n");
    it_find = assoc_find(KEY, sizeof(KEY) - sizeof(""));
    TASSERT(it_find == NULL);

    V_LPRINTF(3, "free\n");
    do_item_deref(it);

    /* check the free list */
    V_LPRINTF(3, "free list count\n");
    TASSERT(fsi.large_free_list_sz == freelist_sz);
    TASSERT(fsi.small_free_list_sz == 0);


    /**
     * small
     */
    V_LPRINTF(2, "small\n");
    V_LPRINTF(3, "allocate\n");
    it = do_item_alloc(KEY, sizeof(KEY) - sizeof(""),
                       FLAGS, current_time + 10000, 0,
                       addr);
    TASSERT(it != NULL);
    TASSERT(is_item_large_chunk(it) == false);
    TASSERT((it->empty_header.it_flags & ITEM_LINKED) == 0);
    TASSERT(it->empty_header.refcount == 1);

    chunk = (chunk_t*) it;
    TASSERT(&chunk->sc.sc_title == &it->small_title);

    V_LPRINTF(3, "link\n");
    do_item_link(it);
    TASSERT((it->empty_header.it_flags & ITEM_LINKED) != 0);
    TASSERT(it->empty_header.refcount == 1);
    TASSERT(lru_check());

    V_LPRINTF(3, "find\n");
    it_find = assoc_find(KEY, sizeof(KEY) - sizeof(""));
    TASSERT(it_find == it);

    V_LPRINTF(3, "unlink\n");
    do_item_unlink(it, UNLINK_NORMAL);
    TASSERT((it->empty_header.it_flags & ITEM_LINKED) == 0);
    TASSERT(it->empty_header.refcount == 1);
    TASSERT(lru_check());

    /* make sure we didn't free it while the refcount == 1 */
    TASSERT(chunk->sc.flags == (SMALL_CHUNK_INITIALIZED | SMALL_CHUNK_USED | SMALL_CHUNK_TITLE));

    V_LPRINTF(3, "find\n");
    it_find = assoc_find(KEY, sizeof(KEY) - sizeof(""));
    TASSERT(it_find == NULL);

    V_LPRINTF(3, "free\n");
    do_item_deref(it);

    /* check the free list */
    V_LPRINTF(3, "free list count\n");
    TASSERT(fsi.large_free_list_sz == freelist_sz);
    TASSERT(fsi.small_free_list_sz == 0);

    return 0;
}


/* allocate an item, link it, and then remove our reference.  check that the
 * object is still allocated.  Finally, unlink the object.  test that it is
 * released.  check refcounts, chunk flags, and free list.  then repeat with
 * a small item. */
static int
deref_unlink_test(int verbose) {
    size_t freelist_sz = fsi.large_free_list_sz;
    size_t min_size_for_large_chunk = ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
        ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 1) * sizeof( ((small_body_chunk_t*) 0)->data ) ) +
        1;
    item* it;
    chunk_t* chunk;
    large_chunk_t* lc_freelist_walk, * pc;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    /**
     * large
     */
    V_LPRINTF(2, "large\n");
    V_LPRINTF(3, "allocate\n");
    min_size_for_large_chunk -= (sizeof(KEY) - sizeof(""));
    it = do_item_alloc(KEY, sizeof(KEY) - sizeof(""),
                       FLAGS, current_time + 10000, min_size_for_large_chunk,
                       addr);
    TASSERT(it != NULL);
    TASSERT(is_item_large_chunk(it));
    TASSERT((it->empty_header.it_flags & ITEM_LINKED) == 0);
    TASSERT(it->empty_header.refcount == 1);

    chunk = (chunk_t*) it;
    TASSERT(chunk->lc.flags == (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_USED | LARGE_CHUNK_TITLE));

    // add the object to the assoc.
    V_LPRINTF(3, "link\n");
    do_item_link(it);
    TASSERT(it == assoc_find(KEY, sizeof(KEY) - sizeof("")));

    // release our reference (but not really).  make sure the chunk remains
    // allocated and we still can get to it.
    V_LPRINTF(3, "deref\n");
    do_item_deref(it);
    TASSERT(chunk->lc.flags == (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_USED | LARGE_CHUNK_TITLE));
    TASSERT(it->empty_header.refcount == 0);
    TASSERT(fsi.large_free_list_sz < freelist_sz);
    TASSERT(it == assoc_find(KEY, sizeof(KEY) - sizeof("")));

    // unlink the object
    V_LPRINTF(3, "unlink\n");
    do_item_unlink(it, UNLINK_NORMAL);
    TASSERT(chunk->lc.flags == (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_FREE));
    TASSERT(NULL == assoc_find(KEY, sizeof(KEY) - sizeof("")));
    TASSERT(fsi.large_free_list_sz == freelist_sz);

    // make sure we get put back on the free list.
    for (lc_freelist_walk = fsi.large_free_list;
         lc_freelist_walk != NULL;
         lc_freelist_walk = lc_freelist_walk->lc_free.next) {
        if (lc_freelist_walk == &chunk->lc) {
            break;
        }
    }
    TASSERT(lc_freelist_walk != NULL);
    TASSERT(fa_freelist_check(LARGE_CHUNK));

    /**
     * small
     */
    V_LPRINTF(2, "small\n");
    V_LPRINTF(3, "allocate\n");
    it = do_item_alloc(KEY, sizeof(KEY) - sizeof(""),
                       FLAGS, current_time + 10000, 0,
                       addr);
    TASSERT(it != NULL);
    TASSERT(is_item_large_chunk(it) == false);
    TASSERT((it->empty_header.it_flags & ITEM_LINKED) == 0);
    TASSERT(it->empty_header.refcount == 1);

    chunk = (chunk_t*) it;
    TASSERT(chunk->sc.flags == (SMALL_CHUNK_INITIALIZED | SMALL_CHUNK_USED | SMALL_CHUNK_TITLE));

    // add the object to the assoc.
    V_LPRINTF(3, "link\n");
    do_item_link(it);
    TASSERT(it == assoc_find(KEY, sizeof(KEY) - sizeof("")));

    // release our reference (but not really).  make sure the chunk remains
    // allocated and we still can get to it.
    V_LPRINTF(3, "deref\n");
    do_item_deref(it);
    TASSERT(chunk->sc.flags == (SMALL_CHUNK_INITIALIZED | SMALL_CHUNK_USED | SMALL_CHUNK_TITLE));
    TASSERT(it->empty_header.refcount == 0);
    TASSERT(fsi.small_free_list_sz < freelist_sz);
    TASSERT(it == assoc_find(KEY, sizeof(KEY) - sizeof("")));

    // once we unlink the object, we won't be able to check the actual chunk any
    // more, because it is probably coalesced.  get the parent chunk here before
    // we do the coalescing.
    pc = get_parent_chunk(&(chunk->sc));

    // unlink the object
    V_LPRINTF(3, "unlink\n");
    do_item_unlink(it, UNLINK_NORMAL);
    TASSERT(pc->flags == (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_FREE));
    TASSERT(NULL == assoc_find(KEY, sizeof(KEY) - sizeof("")));
    TASSERT(fsi.large_free_list_sz == freelist_sz);
    TASSERT(fsi.small_free_list_sz == 0);

    // make sure we get put back on the free list.
    for (lc_freelist_walk = fsi.large_free_list;
         lc_freelist_walk != NULL;
         lc_freelist_walk = lc_freelist_walk->lc_free.next) {
        if (lc_freelist_walk == pc) {
            break;
        }
    }
    TASSERT(lc_freelist_walk != NULL);
    TASSERT(fa_freelist_check(LARGE_CHUNK));
    TASSERT(fa_freelist_check(SMALL_CHUNK));

    return 0;
}


/*
 * two tests:
 *
 * 1) allocate an item, link it, unlink it, release our reference.  test LRU
 *    membership.  This is the same flow as link_unlink_test(..).
 *
 * 2) allocate an item, link it, remove our reference, unlink the item.  test
 *    LRU membership.  This is the same flow as deref_unlink_test(..).
 */
static int
simple_lru_test(int verbose) {
    size_t min_size_for_large_chunk = ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
        ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 1) * sizeof( ((small_body_chunk_t*) 0)->data ) ) +
        1;
    item* it;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    // test 1
    V_LPRINTF(2, "link_unlink\n");
    V_LPRINTF(3, "allocate\n");
    min_size_for_large_chunk -= (sizeof(KEY) - sizeof(""));
    it = do_item_alloc(KEY, sizeof(KEY) - sizeof(""),
                       FLAGS, current_time + 10000, min_size_for_large_chunk,
                       addr);
    TASSERT(it != NULL);
    TASSERT(is_item_large_chunk(it));
    TASSERT((it->empty_header.it_flags & ITEM_LINKED) == 0);
    TASSERT(it->empty_header.refcount == 1);

    // add the object to the assoc.
    V_LPRINTF(3, "link\n");
    do_item_link(it);
    TASSERT(it == assoc_find(KEY, sizeof(KEY) - sizeof("")));
    TASSERT(find_in_lru_by_item(it));

    // unlink the object.
    V_LPRINTF(3, "unlink\n");
    do_item_unlink(it, UNLINK_NORMAL);
    TASSERT(NULL == assoc_find(KEY, sizeof(KEY) - sizeof("")));
    TASSERT(find_in_lru_by_item(it) == false);

    // release and free the object.
    V_LPRINTF(3, "deref\n");
    do_item_deref(it);

    // test 2
    V_LPRINTF(2, "deref_unlink\n");
    V_LPRINTF(3, "allocate\n");
    min_size_for_large_chunk -= (sizeof(KEY) - sizeof(""));
    it = do_item_alloc(KEY, sizeof(KEY) - sizeof(""),
                       FLAGS, current_time + 10000, min_size_for_large_chunk,
                       addr);
    TASSERT(it != NULL);
    TASSERT(is_item_large_chunk(it));
    TASSERT((it->empty_header.it_flags & ITEM_LINKED) == 0);
    TASSERT(it->empty_header.refcount == 1);

    // add the object to the assoc.
    V_LPRINTF(3, "link\n");
    do_item_link(it);
    TASSERT(it == assoc_find(KEY, sizeof(KEY) - sizeof("")));
    TASSERT(find_in_lru_by_item(it));

    // release our reference.
    V_PRINTF(3, "  *  deref\n");
    do_item_deref(it);
    TASSERT(it == assoc_find(KEY, sizeof(KEY) - sizeof("")));
    TASSERT(find_in_lru_by_item(it));

    // unlink the object.
    V_LPRINTF(3, "unlink\n");
    do_item_unlink(it, UNLINK_NORMAL);
    TASSERT(NULL == assoc_find(KEY, sizeof(KEY) - sizeof("")));
    TASSERT(find_in_lru_by_item(it) == false);

    return 0;
}


/**
 * add many items to the LRU.  ensure that they can all be found.
 */
static int
lru_stress_test(int verbose) {
    typedef struct {
        item* it;
        char key[KEY_MAX_LENGTH];
        uint8_t klen;
        uint8_t in_lru;
        uint8_t small_item;
        int nchunks;
    } lru_stress_key_t;
    int i;
    lru_stress_key_t *keys;
    int keys_to_test = LRU_STRESS_TEST_KEYS;
    size_t max_safe_size = ((TOTAL_MEMORY / LARGE_BODY_CHUNK_DATA_SZ) * LARGE_TITLE_CHUNK_DATA_SZ) / keys_to_test;

    V_LPRINTF(4, "max_safe_size = %lu\n", max_safe_size);

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    // create a bunch of objects and add them to the LRU.
    keys = malloc(sizeof(lru_stress_key_t) * keys_to_test);
    for (i = 0; i < keys_to_test; i ++) {
        size_t data_sz;

        // generate a unique key.
        do {
            keys[i].klen = make_random_key(keys[i].key, KEY_MAX_LENGTH);
        } while (assoc_find(keys[i].key, keys[i].klen));

        data_sz = random() % (max_safe_size - keys[i].klen);

        V_PRINTF(2, "\r  *  allocate key %d of size %lu (%lu/%lu chunks free, %lu bytes free)",
                 i, data_sz, fsi.large_free_list_sz, fsi.small_free_list_sz, fsi.unused_memory);
        V_FLUSH(2);

        keys[i].it = do_item_alloc(keys[i].key, keys[i].klen,
                                   FLAGS, 0, data_sz,
                                   addr);
        TASSERT(keys[i].it);

        keys[i].small_item = !(is_item_large_chunk(keys[i].it));
        keys[i].nchunks = chunks_in_item(keys[i].it);

        do_item_link(keys[i].it);
        keys[i].in_lru = true;
    }
    V_PRINTF(2, "\n");

    TASSERT(lru_check());

    // randomly sweep through and either remove or add to the lru (depending on
    // the current state).  after each run, go through and make sure each item
    // that should be in the LRU is, and each item that should not be in the LRU
    // is not.
    for (i = 0; i < LRU_STRESS_TEST_ITERATIONS; i ++) {
        int j;

        V_PRINTF(2, "\r  *  sweep %d", i);
        V_FLUSH(2);
        for (j = 0; j < keys_to_test; j ++) {
            if (random() % 2) {
                if (keys[j].in_lru) {
                    do_item_unlink(keys[j].it, UNLINK_NORMAL);
                } else {
                    do_item_link(keys[j].it);
                }
                keys[j].in_lru = !keys[j].in_lru;
            }
        }

        // ensure that the LRU is not insane...
        TASSERT(lru_check());

        for (j = 0; j < keys_to_test; j ++) {
            TASSERT(keys[j].in_lru == (find_in_lru_by_item(keys[j].it) ? true : false));
            TASSERT(keys[j].in_lru == (assoc_find(keys[j].key, keys[j].klen) ? true : false));
        }
    }
    V_PRINTF(2, "\n");

    for (i = 0; i < keys_to_test; i ++) {
        V_PRINTF(2, "\r  *  freeing key %d", i);
        V_FLUSH(2);

        if (keys[i].in_lru) {
            do_item_unlink(keys[i].it, UNLINK_NORMAL);
        }

        do_item_deref(keys[i].it);
    }
    V_PRINTF(2, "\n");

    free(keys);

    return 0;
}


/**
 * lru ordering test.  allocate two items, and add both to the lru.  release our
 * references and start "accessing" them using do_item_update(..).  check that
 * the order is as expected.  finally, unlink the items.
 */
static int
lru_ordering_test(int verbose) {
    item* it1, * it2;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    V_LPRINTF(2, "allocate\n");

    it1 = do_item_alloc(KEY "0", sizeof(KEY) - sizeof("") + 1,
                        FLAGS, 0, 0, addr);
    it2 = do_item_alloc(KEY "1", sizeof(KEY) - sizeof("") + 1,
                        FLAGS, 0, 0, addr);
    TASSERT(it1);
    TASSERT(it2);

    V_LPRINTF(2, "lru\n");

    do_item_link(it1);
    do_item_link(it2);

    V_LPRINTF(2, "update 1\n");
    current_time += ITEM_UPDATE_INTERVAL + 1;
    do_item_update(it1);

    // now it1 should be younger than it2 in the LRU.
    TASSERT(check_lru_order(it1, it2) == 0);

    V_LPRINTF(2, "update 2\n");
    do_item_update(it2);

    // now it2 should be younger than it1 in the LRU.
    TASSERT(check_lru_order(it2, it1) == 0);

    V_LPRINTF(2, "update 3\n");
    current_time += 2;
    do_item_update(it1);

    // it2 should still be younger than it1 in the LRU because current_time has
    // not moved more than ITEM_UPDATE_INTERVAL.
    TASSERT(check_lru_order(it2, it1) == 0);

    V_LPRINTF(2, "clean up\n");

    do_item_unlink(it1, UNLINK_NORMAL);
    do_item_unlink(it2, UNLINK_NORMAL);

    do_item_deref(it1);
    do_item_deref(it2);

    return 0;
}


static void random_shuffle(item** item_array, size_t item_count) {
    size_t i;
    item* temp;

    for (i = item_count - 1; i > 0; i --) {
        size_t k = random() % i;

        temp = item_array[k];
        item_array[k] = item_array[i];
        item_array[i] = temp;
    }
}


/**
 * allocate a bunch of items.  add them all to the LRU.  verify the initial
 * order.  shuffle the order and access them all in order.  verify the final
 * ordering.  clean up.
 */
static int
lru_ordering_stress_test(int verbose) {
    size_t min_size_for_large_chunk = ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
        ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 1) * sizeof( ((small_body_chunk_t*) 0)->data ) ) +
        1;
    item** item_array = (item**) malloc(sizeof(item*) * LRU_ORDERING_STRESS_TEST_ITEMS * 2);
    item** pre_shuffle_array = &item_array[LRU_ORDERING_STRESS_TEST_ITEMS];
    char key[KEY_MAX_LENGTH];
    size_t i, iter;
    size_t large_freelist_sz = fsi.large_free_list_sz;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    V_LPRINTF(2, "allocate\n");
    for (i = 0; i < LRU_ORDERING_STRESS_TEST_ITEMS; i ++) {
        size_t klen = make_random_key(key, KEY_MAX_LENGTH);

        item_array[i] = do_item_alloc(key, klen, FLAGS, 0, min_size_for_large_chunk, addr);
        do_item_link(item_array[i]);
    }

    for (i = 0; i < LRU_ORDERING_STRESS_TEST_ITEMS; i ++) {
        size_t j;

        V_PRINTF(2, "\r  *  initial order check %lu", i);
        V_FLUSH(2);
        for (j = i + 1; j < LRU_ORDERING_STRESS_TEST_ITEMS; j ++) {
            /* the earlier item (in the array) is older */
            TASSERT(check_lru_order(item_array[i], item_array[j]) == 1);
        }
    }
    V_PRINTF(2, "\n");

    for (iter = 0; iter < LRU_ORDERING_STRESS_TEST_ITERATIONS; iter ++) {
        item** order_check;             /* which array do we use to check the order. */
        V_PRINTF(2, "\r  *  shuffle round %lu", iter);
        V_FLUSH(2);

        if (iter % 2) {
            /*
             * shuffle, but order remains the same because we're not bumping the
             * time.  save the pre-shuffle order.
             */
            memcpy(pre_shuffle_array, item_array, sizeof(item*) * LRU_ORDERING_STRESS_TEST_ITEMS);
            order_check = pre_shuffle_array;
        } else {
            current_time += ITEM_UPDATE_INTERVAL + 1;
            order_check = item_array;
        }

        random_shuffle(item_array, LRU_ORDERING_STRESS_TEST_ITEMS);

        for (i = 0; i < LRU_ORDERING_STRESS_TEST_ITEMS; i ++) {
            do_item_update(item_array[i]);
        }

        for (i = 0; i < LRU_ORDERING_STRESS_TEST_ITEMS; i ++) {
            size_t j;

            for (j = i + 1; j < LRU_ORDERING_STRESS_TEST_ITEMS; j ++) {
                assert(check_lru_order(order_check[i], order_check[j]) == 1);
            }
        }
    }
    V_PRINTF(2, "\n");

    V_LPRINTF(2, "clean up\n");
    for (i = 0; i < LRU_ORDERING_STRESS_TEST_ITEMS; i ++) {
        do_item_unlink(item_array[i], UNLINK_NORMAL);
        do_item_deref(item_array[i]);
    }

    TASSERT(fsi.large_free_list_sz == large_freelist_sz);

    free(item_array);

    return 0;
}


static int get_lru_item_test(int verbose) {
    typedef struct {
        item* it;
        char key[KEY_MAX_LENGTH];
        uint8_t klen;
    } get_lru_item_test_key_t;

    size_t min_size_for_large_chunk = ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
        ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 1) * sizeof( ((small_body_chunk_t*) 0)->data ) ) +
        1;
    size_t max_key_size = SMALL_TITLE_CHUNK_DATA_SZ;
    get_lru_item_test_key_t* items = (get_lru_item_test_key_t*) malloc(sizeof(get_lru_item_test_key_t) *
                                                                       GET_LRU_ITEM_TEST_ITEMS);
    ssize_t i;

    size_t large_free_list_sz = fsi.large_free_list_sz, small_free_list_sz = fsi.small_free_list_sz;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    TASSERT(GET_LRU_ITEM_TEST_ITEMS > LRU_SEARCH_DEPTH);

    V_LPRINTF(2, "small lru\n");

    // small loop
    V_LPRINTF(3, "allocate\n");
    for (i = 0; i < GET_LRU_ITEM_TEST_ITEMS; i ++) {
        do {
            items[i].klen = make_random_key(items[i].key, max_key_size);
        } while (assoc_find(items[i].key, items[i].klen));

        items[i].it = do_item_alloc(items[i].key, items[i].klen, FLAGS, 0, 0, addr);
        TASSERT(items[i].it);
        TASSERT(is_item_large_chunk(items[i].it) == false);

        do_item_link(items[i].it);
    }

    // get_lru_item should fail at this point as all items have refcount > 0.
    TASSERT(get_lru_item() == NULL);

    V_LPRINTF(3, "deref'ed item beyond the range of search depth\n");
    do_item_deref(items[LRU_SEARCH_DEPTH].it);
    TASSERT(get_lru_item() == NULL);

    V_LPRINTF(3, "releasing references in reverse order\n");
    for (i = LRU_SEARCH_DEPTH - 1; i >= 0; i --) {
        do_item_deref(items[i].it);
        TASSERT(get_lru_item() == items[i].it);
    }

    // clean up
    V_LPRINTF(3, "cleanup\n");
    for (i = LRU_SEARCH_DEPTH; i < GET_LRU_ITEM_TEST_ITEMS; i ++) {
        do_item_deref(items[i].it);
    }
    for (i = 0; i < GET_LRU_ITEM_TEST_ITEMS; i ++) {
        do_item_unlink(items[i].it, UNLINK_NORMAL);
    }

    TASSERT(GET_LRU_ITEM_TEST_ITEMS > LRU_SEARCH_DEPTH);

    V_LPRINTF(2, "large lru\n");

    // large loop
    V_LPRINTF(3, "allocate\n");
    for (i = 0; i < GET_LRU_ITEM_TEST_ITEMS; i ++) {
        do {
            items[i].klen = make_random_key(items[i].key, KEY_MAX_LENGTH);
        } while (assoc_find(items[i].key, items[i].klen));

        items[i].it = do_item_alloc(items[i].key, items[i].klen, FLAGS, 0,
                                    min_size_for_large_chunk - items[i].klen,
                                    addr);
        TASSERT(items[i].it);
        TASSERT(is_item_large_chunk(items[i].it));

        do_item_link(items[i].it);
    }

    // get_lru_item should fail at this point as all items have refcount > 0.
    TASSERT(get_lru_item() == NULL);

    V_LPRINTF(3, "deref'ed item beyond the range of search depth\n");
    do_item_deref(items[LRU_SEARCH_DEPTH].it);
    TASSERT(get_lru_item() == NULL);

    V_LPRINTF(3, "releasing references in reverse order\n");
    for (i = LRU_SEARCH_DEPTH - 1; i >= 0; i --) {
        do_item_deref(items[i].it);
        TASSERT(get_lru_item() == items[i].it);
    }

    // clean up
    V_LPRINTF(3, "cleanup\n");
    for (i = LRU_SEARCH_DEPTH; i < GET_LRU_ITEM_TEST_ITEMS; i ++) {
        do_item_deref(items[i].it);
    }
    for (i = 0; i < GET_LRU_ITEM_TEST_ITEMS; i ++) {
        do_item_unlink(items[i].it, UNLINK_NORMAL);
    }

    TASSERT(fsi.large_free_list_sz == large_free_list_sz &&
            fsi.small_free_list_sz == small_free_list_sz);

    return 0;
}


tester_info_t tests[] = {
    {link_unlink_test, 1},
    {deref_unlink_test, 1},
    {simple_lru_test, 1},
    {lru_stress_test, 0},
    {lru_ordering_test, 1},
    {lru_ordering_stress_test, 0},
    {get_lru_item_test, 1},
};


#include "main.h"
