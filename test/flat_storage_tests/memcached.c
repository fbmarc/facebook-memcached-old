/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <ctype.h>
#include <stdlib.h>
#include "memcached.h"

settings_t settings;
rel_time_t current_time;
stats_t stats;

#define SPACEx1   " "
#define SPACEx4   SPACEx1  SPACEx1  SPACEx1  SPACEx1
#define SPACEx16  SPACEx4  SPACEx4  SPACEx4  SPACEx4
#define SPACEx64  SPACEx16  SPACEx16  SPACEx16  SPACEx16
#define SPACEx256 SPACEx64 SPACEx64 SPACEx64 SPACEx64
const char indent_str[257] = SPACEx256;
#undef SPACEx1
#undef SPACEx4
#undef SPACEx16
#undef SPACEx64
#undef SPACEx256


/*
 * if the freelist is okay (correct length, no cycles, prev_next pointers
 * correct, flags), return true.
 */
bool fa_freelist_check(const chunk_type_t ctype) {
    int i;

    switch (ctype) {
        case LARGE_CHUNK:
        {
            large_chunk_t* freelist_walk;

            for (i = 0, freelist_walk = fsi.large_free_list;
                 freelist_walk != NULL;
                 i ++, freelist_walk = freelist_walk->lc_free.next) {
                /* more free list chunks than the counter indicates */
                if (i >= fsi.large_free_list_sz) {
                    return 0;
                }

                /* check the flags */
                if (freelist_walk->flags != (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_FREE)) {
                    return 0;
                }
            }
        }
        break;

        case SMALL_CHUNK:
        {
            small_chunk_t* freelist_walk;
            small_chunk_t** expected_prev_next = &fsi.small_free_list;

            for (i = 0, freelist_walk = fsi.small_free_list;
                 freelist_walk != NULL;
                 i ++, freelist_walk = freelist_walk->sc_free.next) {
                /* more free list chunks than the counter indicates */
                if (i >= fsi.small_free_list_sz) {
                    return 0;
                }

                /* check the flags */
                if (freelist_walk->flags != (SMALL_CHUNK_INITIALIZED | SMALL_CHUNK_FREE)) {
                    return 0;
                }

                /* check the pointer to the previous node's next pointer */
                if (freelist_walk->sc_free.prev_next != expected_prev_next) {
                    return 0;
                }
                expected_prev_next = &freelist_walk->sc_free.next;
            }
        }
        break;
    }

    return 1;
}


/*
 * if the lru is okay (prev_next pointers correct, flags, in incrementing-time
 * order), return true.  this function will *not* return if there is a cycle.
 */
bool lru_check(void) {
    item* lru_walk, * lru_prev;
    rel_time_t prev_timestamp = 0xffffffff;

    if (fsi.lru_head != NULL) {
        /* get_item_from_chunk(..) checks the flags of the enclosing chunk of
         * the returned item, but the lru head is never returned by that.  so
         * we manually check it. */
        if (is_item_large_chunk(fsi.lru_head)) {
            chunk_t* enclosing_chunk = get_chunk_from_item(fsi.lru_head);

            if (enclosing_chunk->lc.flags != (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_USED | LARGE_CHUNK_TITLE)) {
                return false;
            }
        } else {
            chunk_t* enclosing_chunk = get_chunk_from_item(fsi.lru_head);

            if (enclosing_chunk->sc.flags != (SMALL_CHUNK_INITIALIZED | SMALL_CHUNK_USED | SMALL_CHUNK_TITLE)) {
                return false;
            }
        }
    }

    for (lru_walk = fsi.lru_head,
             lru_prev = NULL;
         lru_walk != NULL;
         lru_prev = lru_walk,
             lru_walk = get_item_from_chunk(get_chunk_address(lru_walk->empty_header.next))) {
        /* check the timestamp */
        if (prev_timestamp < lru_walk->empty_header.time) {
            return 0;
        }
        prev_timestamp = lru_walk->empty_header.time;

        /* check the pointer to the previous node */
        if (lru_prev != get_item_from_chunk(get_chunk_address(lru_walk->empty_header.prev))) {
            return false;
        }
    }

    /* at the end, should ensure that the last node is the lru tail. */
    if (fsi.lru_tail != lru_prev) {
        return false;
    }

    return true;
}


/*
 * checks an item for correctness (correct number of chunks, no cycles,
 * prev_next and next pointers correct, chunk flags), return true
 */
bool item_chunk_check(const item* it) {
    if (is_item_large_chunk(it)) {
        const chunk_t* chunk = (const chunk_t*) it;
        const large_chunk_t* lc = &(chunk->lc);
        size_t item_chunk_count = chunks_in_item(it);
        size_t counter = 1;
        const chunkptr_t* expected_prev_next;

        /* don't bother checking the item's fields, but ensure that the flags
         * for the chunk holding it is valid.  LARGE_CHUNK_*/
        if (lc->flags !=
            (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_TITLE | LARGE_CHUNK_USED)) {
            return false;
        }

        for (counter = 1,
                 expected_prev_next = &it->large_title.next_chunk,
                 lc = &(get_chunk_address(it->large_title.next_chunk)->lc);
             lc != NULL;
             counter ++,
                 expected_prev_next = &lc->lc_body.next_chunk,
                 lc = &(get_chunk_address(lc->lc_body.next_chunk)->lc)) {
            /* do we have more chunks in the chain than expected? */
            if (counter > item_chunk_count) {
                return false;
            }

            /* flags set correctly? */
            if (lc->flags !=
                (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_USED)) {
                return false;
            }
        }

        if (counter != item_chunk_count) {
            return false;
        }
    } else {
        const chunk_t* chunk = (const chunk_t*) it;
        const small_chunk_t* sc = &(chunk->sc);
        size_t item_chunk_count = chunks_in_item(it);
        size_t counter = 1;
        chunkptr_t expected_prev_chunk;

        /* don't bother checking the item's fields, but ensure that the flags
         * for the chunk holding it is valid.  SMALL_CHUNK_*/
        if (sc->flags !=
            (SMALL_CHUNK_INITIALIZED | SMALL_CHUNK_TITLE | SMALL_CHUNK_USED)) {
            return false;
        }

        for (counter = 1,
                 expected_prev_chunk = get_chunkptr(get_chunk_from_small_chunk_const(sc)),
                 sc = &(get_chunk_address(it->small_title.next_chunk)->sc);
             sc != NULL;
             counter ++,
                 expected_prev_chunk = get_chunkptr(get_chunk_from_small_chunk_const(sc)),
                 sc = &(get_chunk_address(sc->sc_body.next_chunk)->sc)) {
            const large_chunk_t* parent_chunk;


            /* do we have more chunks in the chain than expected? */
            if (counter > item_chunk_count) {
                return false;
            }

            /* flags set correctly? */
            if (sc->flags !=
                (SMALL_CHUNK_INITIALIZED | SMALL_CHUNK_USED)) {
                return false;
            }

            /* back pointer set correctly? */
            if (expected_prev_chunk != sc->sc_body.prev_chunk) {
                return false;
            }

            /* parent set up correctly?  the type casting to get around const
             * is unfortunate but there's no obvious way around that. */
            parent_chunk = get_parent_chunk_const(sc);
            if (parent_chunk == NULL) {
                return false;
            }
            if (parent_chunk->flags !=
                (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_USED | LARGE_CHUNK_BROKEN)) {
                return false;
            }
        }

        if (counter != item_chunk_count) {
            return false;
        }
    }

    return true;
}


const item* find_in_lru_by_funcptr(find_in_lru_funcptr_t comparator,
                                   find_in_lru_context_t context) {
    item* lru_walk;

    for (lru_walk = fsi.lru_head;
         lru_walk != NULL;
         lru_walk = get_item_from_chunk(get_chunk_address(lru_walk->empty_header.next))) {
        chunk_t* enclosing_chunk = get_chunk_from_item(lru_walk);

        /* check the flags */
        if (is_item_large_chunk(lru_walk)) {
            assert(enclosing_chunk->lc.flags == (LARGE_CHUNK_INITIALIZED | LARGE_CHUNK_USED | LARGE_CHUNK_TITLE));
        } else {
            assert(enclosing_chunk->sc.flags == (SMALL_CHUNK_INITIALIZED | SMALL_CHUNK_USED | SMALL_CHUNK_TITLE));
        }

        if (comparator(lru_walk, context)) {
            return lru_walk;
        }
    }

    return NULL;
}


bool find_in_lru_by_item_comparator(const item* item_to_be_tested, find_in_lru_context_t context) {
    return (item_to_be_tested == context.it);
}


/*
 * if both item1 and item2 are in the LRU, return 0 if item1 is younger (i.e.,
 * closer to the head) than item2.  if item2 is younger than item1, return 1.
 * if item1 is not in the LRU but item2 is, return -1.  if item2 is not in the
 * LRU but item1 is, return -2.  if neither are in the LRU, return -3.
 */
int check_lru_order(const item* item1, const item* item2) {
    bool item1_found = false, item2_found = false;

    item* lru_walk;

    for (lru_walk = fsi.lru_head;
         lru_walk != NULL;
         lru_walk = get_item_from_chunk(get_chunk_address(lru_walk->empty_header.next))) {
        if (item1 == lru_walk) {
            item1_found = true;
            if (item2_found) {
                return 1;
            }
        }

        if (item2 == lru_walk) {
            item2_found = true;
            if (item1_found) {
                return 0;
            }
        }
    }

    return (- ( (item1_found ? 1 : 0) | (item2_found ? 2 : 0) ) );
}


int make_random_key(char* key, size_t key_size) {
    int len, i;

    len = (random() % key_size) + 1;
    for (i = 0; i < len; i ++) {
        char kc;
        while (! isalnum((kc = (char) random()))) {
            ;
        }

        key[i] = kc;
    }

    return len;
}


size_t append_to_buffer(char* const buffer_start,
                        const size_t buffer_size,
                        const size_t buffer_off,
                        const size_t reserved,
                        const char* fmt,
                        ...) {
    va_list ap;
    ssize_t written;
    size_t left = buffer_size - buffer_off;

    if (left <= reserved) {
        return buffer_off;
    }

    va_start(ap, fmt);
    written = vsnprintf(&buffer_start[buffer_off], left, fmt, ap);
    va_end(ap);

    if (written < 0) {
        return buffer_off;
    } else if (written >= left) {
        buffer_start[buffer_off] = 0;
        return buffer_off;
    }

    return buffer_off + written;
}
