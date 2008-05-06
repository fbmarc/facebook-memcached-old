/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "assoc.h"
#include "flat_storage.h"
#include "flat_storage_support.h"


struct in_addr addr = { INADDR_NONE };


/* Allocate a large chunk and check the free lists.  Then free the large chunk
 * and check the free lists. */
static int
simple_alloc_dealloc_large_chunk_test(int verbose) {
    size_t freelist_sz;
    item* it;
    size_t min_size_for_large_chunk = ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
        ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 1) * sizeof( ((small_body_chunk_t*) 0)->data ) ) +
        1;
    chunk_t* chunk;
    large_chunk_t* lc_freelist_walk;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    V_LPRINTF(2, "allocate\n");
    freelist_sz = fsi.large_free_list_sz;
    min_size_for_large_chunk -= (sizeof(KEY) - sizeof(""));
    it = do_item_alloc(KEY, sizeof(KEY) - sizeof(""),
                       FLAGS, current_time + 10000, min_size_for_large_chunk,
                       addr);
    TASSERT(it != NULL);
    TASSERT(is_item_large_chunk(it));

    chunk = (chunk_t*) it;
    TASSERT(&chunk->lc.lc_title == &it->large_title);

    TASSERT(item_chunk_check(it));

    /* check that we removed one node from the free list */
    V_LPRINTF(2, "free list count\n");
    TASSERT(fsi.large_free_list_sz == freelist_sz - 1);
    TASSERT(fsi.small_free_list_sz == 0);

    /* check that the free list is still intact */
    V_LPRINTF(2, "free list check\n");
    TASSERT(fa_freelist_check(SMALL_CHUNK) &&
            fa_freelist_check(LARGE_CHUNK));
    for (lc_freelist_walk = fsi.large_free_list;
         lc_freelist_walk != NULL;
         lc_freelist_walk = lc_freelist_walk->lc_free.next) {
        TASSERT(lc_freelist_walk != &chunk->lc);
    }

    /* check that the item is set up correctly. */
    V_LPRINTF(2, "item check\n");
    TASSERT(memcmp(KEY,
                   ITEM_key(it),
                   sizeof(KEY) - sizeof("")) == 0);
    TASSERT(it->large_title.h_next == NULL_ITEM_PTR);
    TASSERT(it->large_title.next == NULL_CHUNKPTR);
    TASSERT(it->large_title.prev == NULL_CHUNKPTR);
    TASSERT((it->large_title.it_flags & (~ITEM_HAS_IP_ADDRESS)) == ITEM_VALID);
    TASSERT(ITEM_exptime(it) == current_time + 10000);
    TASSERT(ITEM_nbytes(it) == min_size_for_large_chunk);
    TASSERT(ITEM_flags(it) == FLAGS);
    TASSERT(ITEM_refcount(it) == 1);
    TASSERT(ITEM_nkey(it) == sizeof(KEY) - sizeof(""));

    /* now free the chunk */
    V_LPRINTF(2, "chunk free\n");
    do_item_deref(it);

    /* check the free list */
    V_LPRINTF(2, "free list count\n");
    TASSERT(fsi.large_free_list_sz == freelist_sz);
    TASSERT(fsi.small_free_list_sz == 0);

    /* check that the free list is still intact */
    V_LPRINTF(2, "free list check\n");
    TASSERT(fa_freelist_check(SMALL_CHUNK) &&
            fa_freelist_check(LARGE_CHUNK));
    for (lc_freelist_walk = fsi.large_free_list;
         lc_freelist_walk != NULL;
         lc_freelist_walk = lc_freelist_walk->lc_free.next) {
        if (lc_freelist_walk == &chunk->lc) {
            break;
        }
    }
    TASSERT(lc_freelist_walk != NULL);

    return 0;
}


/* Allocate a small chunk and check the free lists.  Then free the small chunk
 * and check the free lists.  Ensure that freeing the small chunk coalesced the
 * broken chunk. */
static int
simple_alloc_dealloc_small_chunk_test(int verbose) {
    size_t lc_freelist_sz;
    item* it;
    size_t small_chunk_sz = 1;
    chunk_t* chunk;
    large_chunk_t* lc_freelist_walk, * parent_chunk;
    small_chunk_t* sc_freelist_walk, * sc;
    int i;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    V_LPRINTF(2, "allocate\n");
    lc_freelist_sz = fsi.large_free_list_sz;
    it = do_item_alloc(KEY, sizeof(KEY) - sizeof(""),
                       FLAGS, current_time + 10000, small_chunk_sz,
                       addr);
    TASSERT(it != NULL);
    TASSERT(is_item_large_chunk(it) == false);
    chunk = (chunk_t*) it;
    TASSERT(&chunk->sc.sc_title == &it->small_title);

    TASSERT(item_chunk_check(it));

    /* get the parent chunk.  item_chunk_check has ensured that it is set up
     * properly. */
    parent_chunk = get_parent_chunk(&chunk->sc);

    /* check that we removed one node from the free list */
    V_LPRINTF(2, "free list count\n");
    TASSERT(fsi.large_free_list_sz == lc_freelist_sz - 1);
    TASSERT(fsi.small_free_list_sz == SMALL_CHUNKS_PER_LARGE_CHUNK - 1);

    /* check that the free list is set up properly. */
    V_LPRINTF(2, "small free list integrity\n");
    TASSERT(fa_freelist_check(SMALL_CHUNK));

    /* walk through all the locations that should be free are in the free list. */
    for (i = 0, sc = &(parent_chunk->lc_broken.lbc[i]);
         i < SMALL_CHUNKS_PER_LARGE_CHUNK;
         i ++) {
        for (sc_freelist_walk = fsi.small_free_list;
             sc_freelist_walk != NULL;
             sc_freelist_walk = sc_freelist_walk->sc_free.next) {
            if (sc == sc_freelist_walk) {
                break;
            }
        }

        TASSERT( (sc_freelist_walk == NULL && sc == &(chunk->sc)) ||
                 (sc_freelist_walk != NULL && sc != &(chunk->sc)) );
    }

    /* check that the free list is set up properly. */
    V_LPRINTF(2, "large free list integrity\n");
    TASSERT(fa_freelist_check(SMALL_CHUNK));

    for (lc_freelist_walk = fsi.large_free_list;
         lc_freelist_walk != NULL;
         lc_freelist_walk = lc_freelist_walk->lc_free.next) {
        TASSERT(lc_freelist_walk != parent_chunk);
    }

    /* check that the item is set up correctly. */
    V_LPRINTF(2, "item check\n");
    TASSERT(memcmp(KEY,
                   ITEM_key(it),
                   sizeof(KEY) - sizeof("")) == 0);
    TASSERT(it->large_title.h_next == NULL_ITEM_PTR);
    TASSERT(it->large_title.next == NULL_CHUNKPTR);
    TASSERT(it->large_title.prev == NULL_CHUNKPTR);
    TASSERT((it->large_title.it_flags & ~(ITEM_HAS_IP_ADDRESS)) == ITEM_VALID);
    TASSERT(ITEM_exptime(it) == current_time + 10000);
    TASSERT(ITEM_nbytes(it) == small_chunk_sz);
    TASSERT(ITEM_flags(it) == FLAGS);
    TASSERT(ITEM_refcount(it) == 1);
    TASSERT(ITEM_nkey(it) == sizeof(KEY) - sizeof(""));

    /* now free the chunk */
    V_LPRINTF(2, "chunk free\n");
    do_item_deref(it);

    /* check the free list */
    V_LPRINTF(2, "free list count\n");
    TASSERT(fsi.large_free_list_sz == lc_freelist_sz);
    TASSERT(fsi.small_free_list_sz == 0);

    /* check that the free list is still intact and that the parent node has
     * been returned to the free list. */
    V_LPRINTF(2, "free list check\n");
    TASSERT(fa_freelist_check(SMALL_CHUNK) &&
            fa_freelist_check(LARGE_CHUNK));
    for (lc_freelist_walk = fsi.large_free_list;
         lc_freelist_walk != NULL;
         lc_freelist_walk = lc_freelist_walk->lc_free.next) {
        if (lc_freelist_walk == parent_chunk) {
            break;
        }
    }
    TASSERT(lc_freelist_walk != NULL);

    return 0;
}


/* Allocate two small chunks and check the free lists.  Then free one small
 * chunk and check the free lists.  Free the remaining chunk and ensure that
 * freeing the small chunk coalesced the broken chunk. */
static int
alloc_partial_dealloc_small_chunk_test(int verbose) {
    size_t lc_freelist_sz;
    item* it1, * it2;
    size_t small_chunk_sz = 1;
    chunk_t* chunk1, * chunk2;
    large_chunk_t* lc_freelist_walk, * parent_chunk;
    small_chunk_t* sc_freelist_walk, * sc;
    int i;
    bool found;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    lc_freelist_sz = fsi.large_free_list_sz;

    V_LPRINTF(2, "allocate\n");
    it1 = do_item_alloc(KEY, sizeof(KEY) - sizeof(""),
                        FLAGS, current_time + 10000, small_chunk_sz,
                        addr);
    TASSERT(it1 != NULL);
    TASSERT(is_item_large_chunk(it1) == false);
    chunk1 = (chunk_t*) it1;
    TASSERT(&chunk1->sc.sc_title == &it1->small_title);

    it2 = do_item_alloc(KEY, sizeof(KEY) - sizeof(""),
                        FLAGS, current_time + 10000, small_chunk_sz,
                        addr);
    TASSERT(it2 != NULL);
    TASSERT(is_item_large_chunk(it2) == false);
    chunk2 = (chunk_t*) it2;
    TASSERT(&chunk2->sc.sc_title == &it2->small_title);

    /* ensure that the items are set up correctly. */
    TASSERT(item_chunk_check(it1));
    TASSERT(item_chunk_check(it2));

    /* get the parent chunk and make sure it is set up properly */
    parent_chunk = get_parent_chunk(&chunk1->sc);
    TASSERT(parent_chunk == get_parent_chunk(&chunk2->sc));

    /* check that we removed one node from the free list */
    V_LPRINTF(2, "free list count\n");
    TASSERT(fsi.large_free_list_sz == lc_freelist_sz - 1);
    TASSERT(fsi.small_free_list_sz == SMALL_CHUNKS_PER_LARGE_CHUNK - 2);

    /* check that the free list is set up properly. */
    V_LPRINTF(2, "small free list integrity\n");
    TASSERT(fa_freelist_check(SMALL_CHUNK));

    /* walk through all the locations that should be free are in the free list. */
    for (i = 0, sc = &(parent_chunk->lc_broken.lbc[i]);
         i < SMALL_CHUNKS_PER_LARGE_CHUNK;
         i ++) {
        for (sc_freelist_walk = fsi.small_free_list;
             sc_freelist_walk != NULL;
             sc_freelist_walk = sc_freelist_walk->sc_free.next) {
            if (sc == sc_freelist_walk) {
                break;
            }
        }

        TASSERT( (sc_freelist_walk == NULL &&
                  (sc == &(chunk1->sc) || sc == (&chunk2->sc))) ||
                 (sc_freelist_walk != NULL && sc != &(chunk1->sc) &&
                  sc != &(chunk2->sc)) );
    }

    /* check that the free list is set up properly. */
    V_LPRINTF(2, "large free list integrity\n");
    TASSERT(fa_freelist_check(LARGE_CHUNK));

    for (lc_freelist_walk = fsi.large_free_list;
         lc_freelist_walk != NULL;
         lc_freelist_walk = lc_freelist_walk->lc_free.next) {
        TASSERT(lc_freelist_walk != parent_chunk);
    }

    /* now free one chunk */
    V_LPRINTF(2, "chunk free it1\n");
    do_item_deref(it1);

    /* check the free list */
    V_LPRINTF(2, "free list count\n");
    TASSERT(fsi.large_free_list_sz == lc_freelist_sz - 1);
    TASSERT(fsi.small_free_list_sz == SMALL_CHUNKS_PER_LARGE_CHUNK - 1);

    /* check that the free list is still intact */
    V_LPRINTF(2, "free list check\n");
    TASSERT(fa_freelist_check(SMALL_CHUNK) &&
            fa_freelist_check(LARGE_CHUNK));
    found = false;
    for (sc_freelist_walk = fsi.small_free_list;
         sc_freelist_walk != NULL;
         sc_freelist_walk = sc_freelist_walk->sc_free.next) {
        /* should not find chunk2 in the free list, but we *must* find chunk1 in
         * the free list. */
        TASSERT(sc_freelist_walk != &chunk2->sc);
        if (sc_freelist_walk == &chunk1->sc) {
            found = true;
        }
    }
    TASSERT(found == true);

    /* now free the other chunk */
    V_LPRINTF(2, "chunk free it2\n");
    do_item_deref(it2);

    /* check the free list */
    V_LPRINTF(2, "free list count\n");
    TASSERT(fsi.large_free_list_sz == lc_freelist_sz);
    TASSERT(fsi.small_free_list_sz == 0);

    /* check that the free list is still intact and that the parent node has
     * been returned to the free list. */
    V_LPRINTF(2, "free list check\n");
    TASSERT(fa_freelist_check(SMALL_CHUNK) &&
            fa_freelist_check(LARGE_CHUNK));
    for (lc_freelist_walk = fsi.large_free_list;
         lc_freelist_walk != NULL;
         lc_freelist_walk = lc_freelist_walk->lc_free.next) {
        if (lc_freelist_walk == parent_chunk) {
            break;
        }
    }
    TASSERT(lc_freelist_walk != NULL);

    return 0;
}


/* allocate an item that spans two large chunks.  verify that the item is set up
 * correctly.  then release the item.  ensure that the free lists are managed
 * correctly. */
static int
alloc_dealloc_two_large_chunk_test(int verbose) {
    size_t freelist_sz;
    item* it;
    size_t min_size_for_multi_large_chunk = sizeof( ((large_title_chunk_t*) 0)->data ) + 1;
    chunk_t* chunk, * second_chunk;
    large_chunk_t* lc_freelist_walk;
    bool found, found_second;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    V_LPRINTF(2, "allocate\n");
    freelist_sz = fsi.large_free_list_sz;
    min_size_for_multi_large_chunk -= (sizeof(KEY) - sizeof(""));
    it = do_item_alloc(KEY, sizeof(KEY) - sizeof(""),
                       FLAGS, current_time + 10000, min_size_for_multi_large_chunk,
                       addr);
    TASSERT(it != NULL);
    TASSERT(is_item_large_chunk(it));
    TASSERT(chunks_in_item(it) == 2);

    /* ensure that the chunks are set up properly. */
    chunk = (chunk_t*) it;
    TASSERT(&chunk->lc.lc_title == &it->large_title);
    second_chunk = get_chunk_address(it->large_title.next_chunk);
    TASSERT(item_chunk_check(it));

    /* check that we removed one node from the free list */
    V_LPRINTF(2, "free list count\n");
    TASSERT(fsi.large_free_list_sz == freelist_sz - 2);
    TASSERT(fsi.small_free_list_sz == 0);

    /* check that the free list is still intact */
    V_LPRINTF(2, "free list check\n");
    TASSERT(fa_freelist_check(SMALL_CHUNK) &&
            fa_freelist_check(LARGE_CHUNK));
    for (lc_freelist_walk = fsi.large_free_list;
         lc_freelist_walk != NULL;
         lc_freelist_walk = lc_freelist_walk->lc_free.next) {
        TASSERT(lc_freelist_walk != &chunk->lc);
        TASSERT(lc_freelist_walk != &second_chunk->lc);
    }

    /* check that the item is set up correctly. */
    V_LPRINTF(2, "item check\n");
    TASSERT(memcmp(KEY,
                   ITEM_key(it),
                   sizeof(KEY) - sizeof("")) == 0);
    TASSERT(it->large_title.h_next == NULL_ITEM_PTR);
    TASSERT(it->large_title.next == NULL_CHUNKPTR);
    TASSERT(it->large_title.prev == NULL_CHUNKPTR);
    TASSERT((it->large_title.it_flags & ~(ITEM_HAS_IP_ADDRESS)) == ITEM_VALID);
    TASSERT(ITEM_exptime(it) == current_time + 10000);
    TASSERT(ITEM_nbytes(it) == min_size_for_multi_large_chunk);
    TASSERT(ITEM_flags(it) == FLAGS);
    TASSERT(ITEM_refcount(it) == 1);
    TASSERT(ITEM_nkey(it) == sizeof(KEY) - sizeof(""));

    /* now free the chunk */
    V_LPRINTF(2, "chunk free\n");
    do_item_deref(it);

    /* check the free list */
    V_LPRINTF(2, "free list count\n");
    TASSERT(fsi.large_free_list_sz == freelist_sz);
    TASSERT(fsi.small_free_list_sz == 0);

    /* check that the free list is still intact */
    V_LPRINTF(2, "free list check\n");
    TASSERT(fa_freelist_check(SMALL_CHUNK) &&
            fa_freelist_check(LARGE_CHUNK));
    found = false;
    found_second = false;
    for (lc_freelist_walk = fsi.large_free_list;
         lc_freelist_walk != NULL;
         lc_freelist_walk = lc_freelist_walk->lc_free.next) {
        if (lc_freelist_walk == &chunk->lc) {
            found = true;
        }
        if (lc_freelist_walk == &second_chunk->lc) {
            found_second = true;
        }
        if (found && found_second) {
            break;
        }
    }
    TASSERT(found && found_second);

    /* check free node */
    V_LPRINTF(2, "title chunk check\n");
    TASSERT(lc_freelist_walk->flags == (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_FREE));

    return 0;
}


/* iteratively allocate and free item that spans multiple large chunks.  verify
 * that the item is set up correctly.  then release the item.  ensure that the
 * free lists are managed correctly. */
static int
alloc_dealloc_many_large_chunk_test(int verbose) {
    size_t initial_freelist_sz = fsi.large_free_list_sz;
    item* it;
    size_t allocate = sizeof( ((large_title_chunk_t*) 0)->data );
    char key[KEY_MAX_LENGTH];
    size_t counter;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    for (counter = 1,
             allocate = allocate - sizeof(key);
         allocate < MAX_ITEM_SIZE + sizeof( ((large_body_chunk_t*) 0)->data ) - 1;
         counter ++,
             allocate += sizeof( ((large_body_chunk_t*) 0)->data )) {
        size_t actual_allocate = allocate > MAX_ITEM_SIZE ? MAX_ITEM_SIZE : allocate;

        V_PRINTF(2, "\r  *  allocate key size = %lu", actual_allocate);
        V_FLUSH(2);

        it = do_item_alloc(key, sizeof(key),
                           FLAGS, current_time + 10000, actual_allocate,
                           addr);
        TASSERT(it != NULL);
        TASSERT(fsi.large_free_list_sz == initial_freelist_sz - counter);
        TASSERT(fsi.small_free_list_sz == 0);

        TASSERT(fa_freelist_check(SMALL_CHUNK) &&
                fa_freelist_check(LARGE_CHUNK));
        TASSERT(item_chunk_check(it));

        V_PRINTF(2, "\r  *  deallocate key size = %lu", actual_allocate);
        V_FLUSH(2);
        do_item_deref(it);

        TASSERT(fsi.large_free_list_sz == initial_freelist_sz);
        TASSERT(fsi.small_free_list_sz == 0);

        TASSERT(fa_freelist_check(SMALL_CHUNK) &&
                fa_freelist_check(LARGE_CHUNK));
    }

    V_PRINTF(2, "\n");

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
                           FLAGS, current_time + 10000, min_size_for_large_chunk,
                           addr);
        TASSERT(it != NULL);
        TASSERT(is_item_large_chunk(it));
        TASSERT(chunks_in_item(it) == 1);

        TASSERT(fsi.large_free_list_sz == initial_freelist_sz - counter - 1);
        TASSERT(fsi.small_free_list_sz == 0);

        item_list[counter] = it;

        TASSERT(fa_freelist_check(SMALL_CHUNK));
        TASSERT(fa_freelist_check(LARGE_CHUNK));
        TASSERT(item_chunk_check(it));
    }

    V_PRINTF(2, "\n");

    TASSERT(fsi.large_free_list_sz == 0);

    for (counter = 0;
         counter < initial_freelist_sz;
         counter ++) {
        V_PRINTF(2, "\r  *  deallocate chunk %lu", counter);
        V_FLUSH(2);

        do_item_deref(item_list[counter]);
    }

    TASSERT(fsi.large_free_list_sz == initial_freelist_sz);
    TASSERT(fsi.small_free_list_sz == 0);
    TASSERT(fa_freelist_check(SMALL_CHUNK));
    TASSERT(fa_freelist_check(LARGE_CHUNK));

    V_PRINTF(2, "\n");

    return 0;
}


/* allocate an item that spans two small chunks but remains in one parent.
 * verify that the item is set up correctly.  then release the item.  ensure
 * that the free lists are managed correctly. */
static int
alloc_dealloc_two_small_chunk_single_parent_test(int verbose) {
    size_t lc_freelist_sz;
    item* it;
    size_t two_small_chunks = sizeof( ((small_title_chunk_t*) 0)->data ) + 1;
    chunk_t* chunk, * second_chunk;
    large_chunk_t* lc_freelist_walk, * parent_chunk;
    small_chunk_t* sc_freelist_walk, * sc;
    size_t i;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    V_LPRINTF(2, "allocate\n");
    lc_freelist_sz = fsi.large_free_list_sz;
    two_small_chunks -= (sizeof(KEY) - sizeof(""));
    it = do_item_alloc(KEY, sizeof(KEY) - sizeof(""),
                       FLAGS, current_time + 10000, two_small_chunks,
                       addr);
    TASSERT(it != NULL);
    TASSERT(is_item_large_chunk(it) == false);
    TASSERT(chunks_in_item(it) == 2);

    /* ensure that the chunks are set up properly. */
    chunk = (chunk_t*) it;
    TASSERT(&chunk->sc.sc_title == &it->small_title);
    second_chunk = get_chunk_address(it->small_title.next_chunk);
    TASSERT(item_chunk_check(it));

    /* get the parent chunk.  item_chunk_check has ensured that it is set up
     * properly. */
    parent_chunk = get_parent_chunk(&chunk->sc);
    TASSERT(parent_chunk == get_parent_chunk(&second_chunk->sc));

    /* check that we removed one node from the free list */
    V_LPRINTF(2, "free list count\n");
    TASSERT(fsi.large_free_list_sz == lc_freelist_sz - 1);
    TASSERT(fsi.small_free_list_sz == SMALL_CHUNKS_PER_LARGE_CHUNK - 2);

    /* check that the free list is still intact */
    V_LPRINTF(2, "small free list check\n");
    TASSERT(fa_freelist_check(SMALL_CHUNK));

    /* walk through all the locations that should be free are in the free list. */
    for (i = 0, sc = &(parent_chunk->lc_broken.lbc[i]);
         i < SMALL_CHUNKS_PER_LARGE_CHUNK;
         i ++) {
        bool should_be_found;           /* whether or not the chunk should be
                                         * found in the free list. */
        bool was_found = false;

        if (sc == &chunk->sc ||
            sc == &second_chunk->sc) {
            should_be_found = false;
        } else {
            should_be_found = true;
        }

        for (sc_freelist_walk = fsi.small_free_list;
             sc_freelist_walk != NULL;
             sc_freelist_walk = sc_freelist_walk->sc_free.next) {
            if (sc == sc_freelist_walk) {
                was_found = true;
                break;
            }
        }

        TASSERT( should_be_found == was_found );
    }

    /* check that the free list is still intact */
    V_LPRINTF(2, "large free list check\n");
    TASSERT(fa_freelist_check(LARGE_CHUNK));
    for (lc_freelist_walk = fsi.large_free_list;
         lc_freelist_walk != NULL;
         lc_freelist_walk = lc_freelist_walk->lc_free.next) {
        TASSERT(lc_freelist_walk != parent_chunk);
    }

    /* check that the item is set up correctly. */
    V_LPRINTF(2, "item check\n");
    TASSERT(memcmp(KEY,
                   ITEM_key(it),
                   sizeof(KEY) - sizeof("")) == 0);
    TASSERT(it->large_title.h_next == NULL_ITEM_PTR);
    TASSERT(it->large_title.next == NULL_CHUNKPTR);
    TASSERT(it->large_title.prev == NULL_CHUNKPTR);
    TASSERT((it->large_title.it_flags & ~(ITEM_HAS_IP_ADDRESS)) == ITEM_VALID);
    TASSERT(ITEM_exptime(it) == current_time + 10000);
    TASSERT(ITEM_nbytes(it) == two_small_chunks);
    TASSERT(ITEM_flags(it) == FLAGS);
    TASSERT(ITEM_refcount(it) == 1);
    TASSERT(ITEM_nkey(it) == sizeof(KEY) - sizeof(""));

    /* now free the chunk */
    V_LPRINTF(2, "chunk free\n");
    do_item_deref(it);

    /* check the free list */
    V_LPRINTF(2, "free list count\n");
    TASSERT(fsi.large_free_list_sz == lc_freelist_sz);
    TASSERT(fsi.small_free_list_sz == 0);

    /* check that the free list is still intact */
    V_LPRINTF(2, "free list check\n");
    TASSERT(fa_freelist_check(SMALL_CHUNK) &&
            fa_freelist_check(LARGE_CHUNK));
    for (lc_freelist_walk = fsi.large_free_list;
         lc_freelist_walk != NULL;
         lc_freelist_walk = lc_freelist_walk->lc_free.next) {
        if (lc_freelist_walk == parent_chunk) {
            break;
        }
    }
    TASSERT(lc_freelist_walk);

    /* check free node */
    V_LPRINTF(2, "title chunk check\n");
    TASSERT(lc_freelist_walk->flags == (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_FREE));

    return 0;
}


/* allocate an item that spans two small chunks but span mulitple parent large
 * chunks.  verify that the item is set up correctly.  then release the item.
 * ensure that the free lists are managed correctly.  we need to placeholders
 * to properly split up the block because when we break the second block, those
 * small chunks are placed in the FRONT of the free list.  therefore, we must
 * exhaust those first. */
static int
alloc_dealloc_two_small_chunk_multiple_parent_test(int verbose) {
    size_t lc_freelist_sz;
    item* it, * holder1, * holder2;
    size_t two_small_chunks = sizeof( ((small_title_chunk_t*) 0)->data ) + 1;
    size_t holder_size = sizeof( ((small_title_chunk_t*) 0)->data ) +
        ((SMALL_CHUNKS_PER_LARGE_CHUNK - 2) * sizeof( ((small_body_chunk_t*) 0)->data ));
    chunk_t* chunk, * second_chunk;
    large_chunk_t* parent_chunk;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    V_LPRINTF(2, "allocate\n");
    lc_freelist_sz = fsi.large_free_list_sz;
    two_small_chunks -= (sizeof(KEY) - sizeof(""));
    holder_size -= (sizeof(KEY) - sizeof(""));
    holder1 = do_item_alloc(KEY, sizeof(KEY) - sizeof(""),
                            FLAGS, current_time + 10000, holder_size,
                            addr);
    TASSERT(holder1 != NULL);
    TASSERT(is_item_large_chunk(holder1) == false);
    TASSERT(chunks_in_item(holder1) == (SMALL_CHUNKS_PER_LARGE_CHUNK - 1));

    TASSERT(fsi.large_free_list_sz == lc_freelist_sz - 1);
    TASSERT(fsi.small_free_list_sz == 1);

    holder2 = do_item_alloc(KEY, sizeof(KEY) - sizeof(""),
                            FLAGS, current_time + 10000, holder_size,
                            addr);
    TASSERT(holder2 != NULL);
    TASSERT(is_item_large_chunk(holder2) == false);
    TASSERT(chunks_in_item(holder2) == (SMALL_CHUNKS_PER_LARGE_CHUNK - 1));

    TASSERT(fsi.large_free_list_sz == lc_freelist_sz - 2);
    TASSERT(fsi.small_free_list_sz == 2);

    it = do_item_alloc(KEY, sizeof(KEY) - sizeof(""),
                       FLAGS, current_time + 10000, two_small_chunks,
                       addr);
    TASSERT(it != NULL);
    TASSERT(is_item_large_chunk(it) == false);
    TASSERT(chunks_in_item(it) == 2);

    /* ensure that the chunks are set up properly. */
    chunk = (chunk_t*) it;
    TASSERT(&chunk->sc.sc_title == &it->small_title);
    second_chunk = get_chunk_address(it->small_title.next_chunk);
    TASSERT(item_chunk_check(holder1));
    TASSERT(item_chunk_check(holder2));
    TASSERT(item_chunk_check(it));

    /* get the parent chunk.  item_chunk_check has ensured that it is set up
     * properly. */
    parent_chunk = get_parent_chunk(&chunk->sc);
    TASSERT(parent_chunk != get_parent_chunk(&second_chunk->sc));

    /* check that we removed one node from the free list */
    V_LPRINTF(2, "free list count\n");
    TASSERT(fsi.large_free_list_sz == lc_freelist_sz - 2);
    TASSERT(fsi.small_free_list_sz == 0);

    /* check that the free list is still intact */
    V_LPRINTF(2, "small free list check\n");
    TASSERT(fa_freelist_check(SMALL_CHUNK));

    /* check that the free list is still intact */
    V_LPRINTF(2, "large free list check\n");
    TASSERT(fa_freelist_check(LARGE_CHUNK));

    /* check that the item is set up correctly. */
    V_LPRINTF(2, "item check\n");
    TASSERT(memcmp(KEY,
                   ITEM_key(it),
                   sizeof(KEY) - sizeof("")) == 0);
    TASSERT(it->large_title.h_next == NULL_ITEM_PTR);
    TASSERT(it->large_title.next == NULL_CHUNKPTR);
    TASSERT(it->large_title.prev == NULL_CHUNKPTR);
    TASSERT((it->large_title.it_flags & ~(ITEM_HAS_IP_ADDRESS)) == ITEM_VALID);
    TASSERT(ITEM_exptime(it) == current_time + 10000);
    TASSERT(ITEM_nbytes(it) == two_small_chunks);
    TASSERT(ITEM_flags(it) == FLAGS);
    TASSERT(ITEM_refcount(it) == 1);
    TASSERT(ITEM_nkey(it) == sizeof(KEY) - sizeof(""));

    /* now free the chunk */
    V_LPRINTF(2, "holder chunk free\n");
    do_item_deref(holder1);
    do_item_deref(holder2);

    V_LPRINTF(2, "free list count\n");
    TASSERT(fsi.large_free_list_sz == lc_freelist_sz - 2);
    TASSERT(fsi.small_free_list_sz == (SMALL_CHUNKS_PER_LARGE_CHUNK - 1) * 2);
    TASSERT(fa_freelist_check(SMALL_CHUNK));
    TASSERT(fa_freelist_check(LARGE_CHUNK));

    /* now free the chunk */
    V_LPRINTF(2, "chunk free\n");
    do_item_deref(it);

    /* check the free list */
    V_LPRINTF(2, "free list count\n");
    TASSERT(fsi.large_free_list_sz == lc_freelist_sz);
    TASSERT(fsi.small_free_list_sz == 0);

    /* check that the free list is still intact */
    V_LPRINTF(2, "free list check\n");
    TASSERT(fa_freelist_check(SMALL_CHUNK));
    TASSERT(fa_freelist_check(LARGE_CHUNK));

    return 0;
}


/* iteratively allocate and free item that spans multiple small chunks.  verify
 * that the item is set up correctly.  then release the item.  ensure that the
 * free lists are managed correctly. */
static int
alloc_dealloc_many_small_chunk_test(int verbose) {
    size_t initial_freelist_sz = fsi.large_free_list_sz;
    item* it;
    size_t allocate = sizeof( ((small_title_chunk_t*) 0)->data );
    size_t max_size_for_small_chunks = ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
        ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 1) * sizeof( ((small_body_chunk_t*) 0)->data ) );
    size_t counter;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    for (counter = 1;
         allocate < max_size_for_small_chunks + sizeof( ((small_body_chunk_t*) 0)->data ) - 1;
         counter ++,
             allocate += sizeof( ((small_body_chunk_t*) 0)->data )) {
        size_t actual_allocate = allocate > max_size_for_small_chunks ?
            max_size_for_small_chunks : allocate;

        V_PRINTF(2, "\r  *  allocate key size = %lu", actual_allocate);
        V_FLUSH(2);

        it = do_item_alloc(NULL, 0,
                           FLAGS, current_time + 10000, actual_allocate,
                           addr);
        TASSERT(it != NULL);
        TASSERT(fsi.large_free_list_sz == initial_freelist_sz - 1);
        TASSERT(fsi.small_free_list_sz == SMALL_CHUNKS_PER_LARGE_CHUNK - counter);

        TASSERT(fa_freelist_check(SMALL_CHUNK) &&
                fa_freelist_check(LARGE_CHUNK));
        TASSERT(item_chunk_check(it));

        V_PRINTF(2, "\r  *  deallocate key size = %lu", actual_allocate);
        V_FLUSH(2);
        do_item_deref(it);

        TASSERT(fsi.large_free_list_sz == initial_freelist_sz);
        TASSERT(fsi.small_free_list_sz == 0);

        TASSERT(fa_freelist_check(SMALL_CHUNK) &&
                fa_freelist_check(LARGE_CHUNK));
    }

    V_PRINTF(2, "\n");

    return 0;
}


/* iteratively allocate all available large chunks.  subsequencly free all the
 * itmes.  ensure that the free lists are managed correctly. */
static int
alloc_all_small_chunks_test(int verbose) {
    size_t initial_freelist_sz = fsi.large_free_list_sz;
    item** item_list;
    size_t max_size_for_small_chunks = ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
        ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 2) * sizeof( ((small_body_chunk_t*) 0)->data ) );
    ssize_t counter;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    item_list = malloc(sizeof(item*) * initial_freelist_sz * 2);

    for (counter = 0;
         fsi.large_free_list_sz > 0 || fsi.small_free_list_sz > (SMALL_CHUNKS_PER_LARGE_CHUNK - 1);
         counter ++) {
        item* it;

        V_PRINTF(2, "\r  *  allocate chunk %lu", counter);
        V_FLUSH(2);

        it = do_item_alloc(NULL, 0,
                           FLAGS, current_time + 10000, max_size_for_small_chunks,
                           addr);
        TASSERT(it != NULL);
        TASSERT(is_item_large_chunk(it) == false);
        TASSERT(chunks_in_item(it) == SMALL_CHUNKS_PER_LARGE_CHUNK - 1);

        item_list[counter] = it;

        TASSERT(fa_freelist_check(SMALL_CHUNK));
        TASSERT(fa_freelist_check(LARGE_CHUNK));
        TASSERT(item_chunk_check(it));
    }

    V_PRINTF(2, "\n");

    TASSERT(fsi.large_free_list_sz == 0);

    for (counter = counter - 1;         /* last one has no item. */
         counter >= 0;
         counter --) {
        V_PRINTF(2, "%16s  *  deallocate chunk %lu", "\r", counter);
        V_FLUSH(2);

        do_item_deref(item_list[counter]);
    }

    TASSERT(fsi.large_free_list_sz == initial_freelist_sz);
    TASSERT(fsi.small_free_list_sz == 0);
    TASSERT(fa_freelist_check(SMALL_CHUNK));
    TASSERT(fa_freelist_check(LARGE_CHUNK));

    V_PRINTF(2, "\n");

    return 0;
}


tester_info_t tests[] = {
    {simple_alloc_dealloc_large_chunk_test, 1},
    {simple_alloc_dealloc_small_chunk_test, 1},
    {alloc_partial_dealloc_small_chunk_test, 1},
    {alloc_dealloc_two_large_chunk_test, 1},
    {alloc_dealloc_many_large_chunk_test, 0},
    {alloc_all_large_chunks_test, 1},
    {alloc_dealloc_two_small_chunk_single_parent_test, 1},
    {alloc_dealloc_two_small_chunk_multiple_parent_test, 1},
    {alloc_dealloc_many_small_chunk_test, 1},
    {alloc_all_small_chunks_test, 1},
};


#include "main.h"
