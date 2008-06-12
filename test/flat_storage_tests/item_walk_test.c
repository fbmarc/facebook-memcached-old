/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "assoc.h"
#include "flat_storage.h"
#include "flat_storage_support.h"


struct in_addr addr = { INADDR_NONE };


static int
small_single_chunk_item_test(int verbose) {
    size_t item_size;
    size_t max_size_for_small_single_chunk_item = sizeof( ((small_title_chunk_t*) 0)->data );
    item* it;
    char key[KEY_MAX_LENGTH] = {' '};

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    for (item_size = 0;
         item_size <= max_size_for_small_single_chunk_item;
         item_size ++) {
        size_t key_size;

        V_PRINTF(2, "\r  *  allocate item size = %lu", item_size);
        V_FLUSH(2);
        /* printf("\n"); */

        for (key_size = 0;
             key_size <= item_size && key_size <= KEY_MAX_LENGTH;
             key_size ++) {
            size_t value_size = item_size - key_size;
            size_t walk_start, walk_end;

            it = do_item_alloc(key, key_size, FLAGS, current_time + 10000,
                               value_size, addr);
            TASSERT(it != NULL);

            for (walk_start = 0; walk_start < value_size; walk_start ++) {
                for (walk_end = walk_start; walk_end < value_size; walk_end ++) {
                    size_t range = walk_end - walk_start + 1;

                    /* printf("  walking range %ld-%ld\n", walk_start,
                     * walk_end); */
#define SMALL_SINGLE_CHUNK_ITEM_TEST_APPLIER(_it, _ptr, _bytes)         \
                    /* printf("   _ptr = %p, _bytes = %ld\n", _ptr, _bytes); */ \
                        TASSERT(_it == it);                             \
                        TASSERT(_ptr == &it->small_title.data[key_size + walk_start]); \
                        TASSERT(_bytes == range);

                    ITEM_WALK(it, walk_start, range, false, SMALL_SINGLE_CHUNK_ITEM_TEST_APPLIER, );
#undef SMALL_SINGLE_CHUNK_ITEM_TEST_APPLIER
                }
            }

            do_item_deref(it);
        }
    }

    V_PRINTF(2, "\n");

    return 0;
}


static int
large_single_chunk_item_test(int verbose) {
    size_t item_size;
    size_t max_size_for_large_single_chunk_item = sizeof( ((large_title_chunk_t*) 0)->data );
    item* it;
    char key[KEY_MAX_LENGTH] = {' '};

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    for (item_size = 0;
         item_size <= max_size_for_large_single_chunk_item;
         item_size ++) {
        size_t key_size;

        V_PRINTF(2, "\r  *  allocate item size = %lu", item_size);
        V_FLUSH(2);
        /* printf("\n"); */

        for (key_size = 0;
             key_size <= item_size && key_size <= KEY_MAX_LENGTH;
             key_size ++) {
            size_t value_size = item_size - key_size;
            size_t walk_start, walk_end;

            if (is_large_chunk(key_size, value_size) == false) {
                continue;
            }

            it = do_item_alloc(key, key_size, FLAGS, current_time + 10000,
                               value_size, addr);
            TASSERT(it != NULL);

            for (walk_start = 0; walk_start < value_size; walk_start ++) {
                for (walk_end = walk_start; walk_end < value_size; walk_end ++) {
                    size_t range = walk_end - walk_start + 1;

                    /* printf("  walking range %ld-%ld\n", walk_start,
                     * walk_end); */
#define LARGE_SINGLE_CHUNK_ITEM_TEST_APPLIER(_it, _ptr, _bytes)         \
                    /* printf("   _ptr = %p, _bytes = %ld\n", _ptr, _bytes); */ \
                        TASSERT(_it == it);                             \
                        TASSERT(_ptr == &it->large_title.data[key_size + walk_start]); \
                        TASSERT(_bytes == range);

                    ITEM_WALK(it, walk_start, range, false, LARGE_SINGLE_CHUNK_ITEM_TEST_APPLIER, );
#undef LARGE_SINGLE_CHUNK_ITEM_TEST_APPLIER
                }
            }

            do_item_deref(it);
        }
    }

    V_PRINTF(2, "\n");

    return 0;
}


/* like small_single_chunk_item_test, except we only cover ranges that extend
 * beyond the item boundary. */
static int
beyond_small_single_chunk_item_test(int verbose) {
    size_t item_size;
    size_t max_size_for_small_single_chunk_item = sizeof( ((small_title_chunk_t*) 0)->data );
    item* it;
    char key[KEY_MAX_LENGTH] = {' '};

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    for (item_size = 0;
         item_size <= max_size_for_small_single_chunk_item;
         item_size ++) {
        size_t key_size;

        V_PRINTF(2, "\r  *  allocate item size = %lu", item_size);
        V_FLUSH(2);
        /* printf("\n"); */

        for (key_size = 0;
             key_size <= item_size && key_size <= KEY_MAX_LENGTH;
             key_size ++) {
            size_t value_size = item_size - key_size;
            size_t walk_start, walk_end;

            it = do_item_alloc(key, key_size, FLAGS, current_time + 10000,
                               value_size, addr);
            TASSERT(it != NULL);

            for (walk_start = 0;
                 walk_start < max_size_for_small_single_chunk_item - key_size;
                 walk_start ++) {
                for (walk_end = value_size;
                     walk_end < max_size_for_small_single_chunk_item - key_size;
                     walk_end ++) {
                    size_t range = walk_end - walk_start + 1;

                    /* printf("  walking range %ld-%ld\n", walk_start,
                     * walk_end); */
#define SMALL_SINGLE_CHUNK_ITEM_TEST_APPLIER(_it, _ptr, _bytes)         \
                    /* printf("   _ptr = %p, _bytes = %ld\n", _ptr, _bytes); */ \
                        TASSERT(_it == it);                             \
                        TASSERT(_ptr == &it->small_title.data[key_size + walk_start]); \
                        TASSERT(_bytes == range);

                    ITEM_WALK(it, walk_start, range, true, SMALL_SINGLE_CHUNK_ITEM_TEST_APPLIER, );
#undef SMALL_SINGLE_CHUNK_ITEM_TEST_APPLIER
                }
            }

            do_item_deref(it);
        }
    }

    V_PRINTF(2, "\n");

    return 0;
}


/* like large_single_chunk_item_test, except for ranges that extend beyond the
 * item boundary.  because this is such a huge space, we only check a few
 * corner cases. */
static int
beyond_large_single_chunk_item_test(int verbose) {
    size_t item_size;
    size_t max_size_for_large_single_chunk_item = sizeof( ((large_title_chunk_t*) 0)->data );
    item* it;
    char key[KEY_MAX_LENGTH] = {' '};

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    for (item_size = 0;
         item_size <= max_size_for_large_single_chunk_item;
         item_size ++) {
        size_t key_size;

        V_PRINTF(2, "\r  *  allocate item size = %lu", item_size);
        V_FLUSH(2);
        /* printf("\n"); */

        for (key_size = 0;
             key_size <= item_size && key_size <= KEY_MAX_LENGTH;
             key_size ++) {
            size_t value_size = item_size - key_size;
            size_t walk_start, walk_end;

            if (is_large_chunk(key_size, value_size) == false) {
                continue;
            }

            it = do_item_alloc(key, key_size, FLAGS, current_time + 10000,
                               value_size, addr);
            TASSERT(it != NULL);

            /* doing an exhaustive coverage of the entire beyond_item_boundary
             * space will take quite a while.  thus, we can just cover a few
             * corner cases. */

            /* start at the start of the value, go one byte beyond the
             * boundary. */
            walk_start = 0;
            walk_end = value_size;

            TASSERT(walk_end <= max_size_for_large_single_chunk_item - key_size - 1 ||
                    item_slackspace(it) == 0);

            /* one byte beyond the boundary could theoretically be beyond the
             * chunk, so we don't check those cases. */
            if (walk_end <= max_size_for_large_single_chunk_item - key_size - 1) {
#define LARGE_SINGLE_CHUNK_ITEM_TEST_APPLIER(_it, _ptr, _bytes)         \
                /* printf("   _ptr = %p, _bytes = %ld\n", _ptr, _bytes); */ \
                    TASSERT(_it == it);                                 \
                    TASSERT(_ptr == &it->large_title.data[key_size + walk_start]); \
                    TASSERT(_bytes == (walk_end - walk_start + 1));

                ITEM_WALK(it, walk_start, walk_end - walk_start + 1, true, LARGE_SINGLE_CHUNK_ITEM_TEST_APPLIER, );
#undef LARGE_SINGLE_CHUNK_ITEM_TEST_APPLIER
            }

            /* start at the start of the value, go to the last byte of the
             * chunk. */
            walk_start = 0;
            walk_end = max_size_for_large_single_chunk_item - key_size - 1;

#define LARGE_SINGLE_CHUNK_ITEM_TEST_APPLIER(_it, _ptr, _bytes)         \
            /* printf("   _ptr = %p, _bytes = %ld\n", _ptr, _bytes); */ \
                TASSERT(_it == it);                                     \
                TASSERT(_ptr == &it->large_title.data[key_size + walk_start]); \
                TASSERT(_bytes == (walk_end - walk_start + 1));

            ITEM_WALK(it, walk_start, walk_end - walk_start + 1, true, LARGE_SINGLE_CHUNK_ITEM_TEST_APPLIER, );
#undef LARGE_SINGLE_CHUNK_ITEM_TEST_APPLIER

            /* start at the end of the value, go one byte beyond the boundary.
             */
            walk_start = value_size;
            walk_end = value_size;

            TASSERT(walk_end <= max_size_for_large_single_chunk_item - key_size - 1 ||
                    item_slackspace(it) == 0);

            /* one byte beyond the boundary could theoretically be beyond the
             * chunk, so we don't check those cases. */
            if (walk_end <= max_size_for_large_single_chunk_item - key_size - 1) {
#define LARGE_SINGLE_CHUNK_ITEM_TEST_APPLIER(_it, _ptr, _bytes)         \
                /* printf("   _ptr = %p, _bytes = %ld\n", _ptr, _bytes); */ \
                    TASSERT(_it == it);                                 \
                    TASSERT(_ptr == &it->large_title.data[key_size + walk_start]); \
                    TASSERT(_bytes == (walk_end - walk_start + 1));

                ITEM_WALK(it, walk_start, walk_end - walk_start + 1, true, LARGE_SINGLE_CHUNK_ITEM_TEST_APPLIER, );
#undef LARGE_SINGLE_CHUNK_ITEM_TEST_APPLIER
            }

            /* start at the end of the value, go to the last byte of the
             * chunk. */
            walk_start = value_size;
            walk_end = max_size_for_large_single_chunk_item - key_size - 1;

#define LARGE_SINGLE_CHUNK_ITEM_TEST_APPLIER(_it, _ptr, _bytes)         \
            /* printf("   _ptr = %p, _bytes = %ld\n", _ptr, _bytes); */ \
                TASSERT(_it == it);                                     \
                TASSERT(_ptr == &it->large_title.data[key_size + walk_start]); \
                TASSERT(_bytes == (walk_end - walk_start + 1));

            ITEM_WALK(it, walk_start, walk_end - walk_start + 1, true, LARGE_SINGLE_CHUNK_ITEM_TEST_APPLIER, );
#undef LARGE_SINGLE_CHUNK_ITEM_TEST_APPLIER

            do_item_deref(it);
        }
    }

    V_PRINTF(2, "\n");

    return 0;
}


static void
clear_item(item* it) {
    if (is_item_large_chunk(it)) {
        large_body_chunk_t* body;

        memset(it->large_title.data, 0, sizeof(it->large_title.data));
        body = &(get_chunk_address(it->large_title.next_chunk)->lc.lc_body);

        while (body) {
            memset(body->data, 0, sizeof(body->data));
            body = &(get_chunk_address(body->next_chunk)->lc.lc_body);
        }
    } else {
        small_body_chunk_t* body;

        memset(it->small_title.data, 0, sizeof(it->small_title.data));
        body = &(get_chunk_address(it->small_title.next_chunk)->sc.sc_body);

        while (body) {
            memset(body->data, 0, sizeof(body->data));
            body = &(get_chunk_address(body->next_chunk)->sc.sc_body);
        }
    }
}


static char*
item_copy_out(item* it) {
    char* buffer, * dst;

    buffer = dst = malloc(it->empty_header.nkey + it->empty_header.nbytes +
                          LARGE_BODY_CHUNK_DATA_SZ);

    /* copy out the entirety of all the item blocks. */
    if (is_item_large_chunk(it)) {
        large_body_chunk_t* body;

        memcpy(dst, it->large_title.data, sizeof(it->large_title.data));
        dst += sizeof(it->large_title.data);
        body = &(get_chunk_address(it->large_title.next_chunk)->lc.lc_body);

        while (body) {
            memcpy(dst, body->data, sizeof(body->data));
            dst += sizeof(body->data);
            body = &(get_chunk_address(body->next_chunk)->lc.lc_body);
        }
    } else {
        small_body_chunk_t* body;

        memcpy(dst, it->small_title.data, sizeof(it->small_title.data));
        dst += sizeof(it->small_title.data);
        body = &(get_chunk_address(it->small_title.next_chunk)->sc.sc_body);

        while (body) {
            memcpy(dst, body->data, sizeof(body->data));
            dst += sizeof(body->data);
            body = &(get_chunk_address(body->next_chunk)->sc.sc_body);
        }
    }

    return buffer;
}


static int
byte_compare(const char* buffer, ssize_t start, ssize_t end, char c) {
    for (;
         start <= end;
         start ++) {
        if (buffer[start] != c) {
            return buffer[start] - c;
        }
    }

    return 0;
}


/* hit a few of the corner cases in item size and key size for multi chunk small
 * copy out the entirety of all the item blocksitems.  sweep through all
 * possible walking ranges. */
static int
small_multi_chunk_item_test(int verbose) {
    size_t num_chunks;
    item* it;
    char key[KEY_MAX_LENGTH] = {' '};

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    for (num_chunks = 2;
         num_chunks < SMALL_CHUNKS_PER_LARGE_CHUNK;
         num_chunks ++) {
        /* item_size_min is the minimum item size to require num_chunks chunks.
         * item_size_max is the maximum item size to require num_chunks chunks.
         */
        size_t item_size_min = sizeof( ((small_title_chunk_t*) 0)->data ) +
            ((num_chunks - 2) * sizeof( ((small_body_chunk_t*) 0)->data )) + 1;
        size_t item_size_max = sizeof( ((small_title_chunk_t*) 0)->data ) +
            ((num_chunks - 1) * sizeof( ((small_body_chunk_t*) 0)->data ));
        size_t item_sizes[] = { item_size_min,
                                item_size_min + 1,
                                item_size_max - 1,
                                item_size_max };
        int i;

        for (i = 0; i < (sizeof(item_sizes) / sizeof(*item_sizes)); i ++) {
            size_t item_size = item_sizes[i];
            size_t key_size_min = 0;
            size_t key_size_max = (KEY_MAX_LENGTH > sizeof( ((small_title_chunk_t*) 0)->data )) ?
                (sizeof( ((small_title_chunk_t*) 0)->data )) : (KEY_MAX_LENGTH);
            size_t key_sizes[] = { key_size_min,
                                   key_size_min + 1,
                                   key_size_max - 1,
                                   key_size_max };
            int j;

            V_PRINTF(2, "\r  *  allocate item size = %lu", item_size);
            V_FLUSH(2);

            for (j = 0; j < (sizeof(key_sizes) / sizeof(*key_sizes)); j ++) {
                size_t key_size = key_sizes[j];
                size_t value_size = item_size - key_size;
                size_t walk_start, walk_end;

                it = do_item_alloc(key, key_size, FLAGS, current_time + 10000,
                                   value_size, addr);
                TASSERT(it != NULL);

                for (walk_start = 0; walk_start < value_size; walk_start ++) {
                    for (walk_end = walk_start; walk_end < value_size; walk_end ++) {
                        size_t range = walk_end - walk_start + 1;
                        char* copy;

                        clear_item(it);

                        /*printf(" walking range %ld-%ld\n", walk_start,
                          walk_end); */
#define SMALL_MULTI_CHUNK_ITEM_TEST_APPLIER(_it, _ptr, _bytes)         \
                        /* printf("   _ptr = %p, _bytes = %ld\n", _ptr, _bytes); */  \
                        TASSERT(_it == it);                             \
                        memset(_ptr, 0x5a, _bytes);

                        ITEM_WALK(it, walk_start, range, false, SMALL_MULTI_CHUNK_ITEM_TEST_APPLIER, );
#undef SMALL_MULTI_CHUNK_ITEM_TEST_APPLIER

                        copy = item_copy_out(it);

                        TASSERT(byte_compare(copy, 0, key_size - 1, 0) == 0);
                        TASSERT(byte_compare(copy, key_size, key_size + walk_start - 1, 0) == 0);
                        TASSERT(byte_compare(copy, key_size + walk_start, key_size + walk_end, 0x5a) == 0);
                        TASSERT(byte_compare(copy, key_size + walk_end + 1, item_size - 1, 0) == 0);

                        free(copy);
                    }
                }

                do_item_deref(it);
            }
        }
    }

    V_PRINTF(2, "\n");

    return 0;
}


/* hit a few of the corner cases in item size and key size for multi chunk large
 * copy out the entirety of all the item blocksitems.  since this search space
 * is so much larger, we only hit a few corner cases to try. */
static int
large_multi_chunk_item_test(int verbose) {
    size_t num_chunks;
    item* it;
    char key[KEY_MAX_LENGTH] = {' '};

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    for (num_chunks = 2;
         num_chunks < 5;
         num_chunks ++) {
        /* item_size_min is the minimum item size to require num_chunks chunks.
         * item_size_max is the maximum item size to require num_chunks chunks.
         */
        size_t item_size_min = sizeof( ((large_title_chunk_t*) 0)->data ) +
            ((num_chunks - 2) * sizeof( ((large_body_chunk_t*) 0)->data )) + 1;
        size_t item_size_max = sizeof( ((large_title_chunk_t*) 0)->data ) +
            ((num_chunks - 1) * sizeof( ((large_body_chunk_t*) 0)->data ));
        size_t item_sizes[] = { item_size_min,
                                item_size_min + 1,
                                item_size_max - 1,
                                item_size_max };
        int i;

        for (i = 0; i < (sizeof(item_sizes) / sizeof(*item_sizes)); i ++) {
            size_t item_size = item_sizes[i];
            size_t key_size_min = 0;
            size_t key_size_max = (KEY_MAX_LENGTH > sizeof( ((large_title_chunk_t*) 0)->data )) ?
                (sizeof( ((large_title_chunk_t*) 0)->data )) : (KEY_MAX_LENGTH);
            size_t key_sizes[] = { key_size_min,
                                   key_size_min + 1,
                                   key_size_max - 1,
                                   key_size_max };
            int j;

            V_PRINTF(2, "\r  *  allocate item size = %lu", item_size);
            V_FLUSH(2);

            for (j = 0; j < (sizeof(key_sizes) / sizeof(*key_sizes)); j ++) {
                size_t key_size = key_sizes[j];
                size_t value_size = item_size - key_size;
                size_t walk_start, walk_end;
                char* copy;
                int cntr;

                it = do_item_alloc(key, key_size, FLAGS, current_time + 10000,
                                   value_size, addr);
                TASSERT(it != NULL);

                /******
                 * case 1: start at the start of the value, go right up to the
                 * end of the value. */
                walk_start = 0;
                walk_end = value_size - 1;

                clear_item(it);

                /*printf(" walking range %ld-%ld\n", walk_start,
                  walk_end); */
#define LARGE_MULTI_CHUNK_ITEM_TEST_APPLIER(_it, _ptr, _bytes)         \
                /* printf("   _ptr = %p, _bytes = %ld\n", _ptr, _bytes); */ \
                    TASSERT(_it == it);                                 \
                    memset(_ptr, 0x5a, _bytes);

                ITEM_WALK(it, walk_start, walk_end - walk_start + 1, false, LARGE_MULTI_CHUNK_ITEM_TEST_APPLIER, );
#undef LARGE_MULTI_CHUNK_ITEM_TEST_APPLIER

                copy = item_copy_out(it);

                TASSERT(byte_compare(copy, 0, key_size - 1, 0) == 0);
                TASSERT(byte_compare(copy, key_size, key_size + walk_start - 1, 0) == 0);
                TASSERT(byte_compare(copy, key_size + walk_start, key_size + walk_end, 0x5a) == 0);
                TASSERT(byte_compare(copy, key_size + walk_end + 1, item_size - 1, 0) == 0);

                free(copy);

                /******
                 * case 2: start at the start of the value, copy everything
                 * except the last block. */
                walk_start = 0;

                /* calculate the number of bytes left in the last block */
                walk_end = value_size;
                if (walk_end >= (LARGE_TITLE_CHUNK_DATA_SZ - key_size)) {
                    walk_end -= (LARGE_TITLE_CHUNK_DATA_SZ - key_size);
                }

                walk_end %= LARGE_BODY_CHUNK_DATA_SZ;
                if (walk_end == 0) {
                    /* we fit exactly into the body chunks.  so to exempt the
                     * last chunk, chop off LARGE_BODY_CHUNK_DATA_SZ. */
                    walk_end = LARGE_BODY_CHUNK_DATA_SZ;
                }

                /* so walk over all bytes except for the leftover bytes. */
                walk_end = value_size - walk_end;
                walk_end --;            /* length to offset conversion... */

                clear_item(it);

                cntr = 0;

/*                 printf(" value size = %ld\n", value_size); */
/*                 printf(" walking range %ld-%ld\n", walk_start, */
/*                        walk_end); */
#define LARGE_MULTI_CHUNK_ITEM_TEST_APPLIER(_it, _ptr, _bytes)         \
                /* printf("   _ptr = %p, _bytes = %ld\n", _ptr, _bytes); */ \
                    TASSERT(_it == it);                                 \
                    memset(_ptr, 0x5a, _bytes);                         \
                    if (cntr == 0) {                                    \
                        TASSERT(_ptr >= &it->large_title.data[0] &&     \
                                _ptr <= &it->large_title.data[LARGE_TITLE_CHUNK_DATA_SZ - 1]); \
                    } else {                                            \
                        TASSERT(_ptr < &it->large_title.data[0] ||      \
                                _ptr > &it->large_title.data[LARGE_TITLE_CHUNK_DATA_SZ - 1]); \
                    }                                                   \
                                                                        \
                    cntr ++;

                ITEM_WALK(it, walk_start, walk_end - walk_start + 1, false, LARGE_MULTI_CHUNK_ITEM_TEST_APPLIER, );
#undef LARGE_MULTI_CHUNK_ITEM_TEST_APPLIER

                TASSERT(cntr == num_chunks - 1);

                copy = item_copy_out(it);

                TASSERT(byte_compare(copy, 0, key_size - 1, 0) == 0);
                TASSERT(byte_compare(copy, key_size, key_size + walk_start - 1, 0) == 0);
                TASSERT(byte_compare(copy, key_size + walk_start, key_size + walk_end, 0x5a) == 0);
                TASSERT(byte_compare(copy, key_size + walk_end + 1, item_size - 1, 0) == 0);

                free(copy);

                /******
                 * case 3: start at the start of the first body block, go right
                 * up to the end of the value. */
                walk_start = LARGE_TITLE_CHUNK_DATA_SZ - key_size;
                walk_end = value_size - 1;

                clear_item(it);

                cntr = 0;

                /*printf(" walking range %ld-%ld\n", walk_start,
                  walk_end); */
#define LARGE_MULTI_CHUNK_ITEM_TEST_APPLIER(_it, _ptr, _bytes)         \
                /* printf("   _ptr = %p, _bytes = %ld\n", _ptr, _bytes); */ \
                    TASSERT(_it == it);                                 \
                    memset(_ptr, 0x5a, _bytes);                         \
                    TASSERT(_ptr < &it->large_title.data[0] ||          \
                            _ptr > &it->large_title.data[LARGE_TITLE_CHUNK_DATA_SZ - 1]); \
                    cntr ++;

                ITEM_WALK(it, walk_start, walk_end - walk_start + 1, false, LARGE_MULTI_CHUNK_ITEM_TEST_APPLIER, );
#undef LARGE_MULTI_CHUNK_ITEM_TEST_APPLIER

                TASSERT(cntr == num_chunks - 1);

                copy = item_copy_out(it);

                TASSERT(byte_compare(copy, 0, key_size - 1, 0) == 0);
                TASSERT(byte_compare(copy, key_size, key_size + walk_start - 1, 0) == 0);
                TASSERT(byte_compare(copy, key_size + walk_start, key_size + walk_end, 0x5a) == 0);
                TASSERT(byte_compare(copy, key_size + walk_end + 1, item_size - 1, 0) == 0);

                free(copy);

                /******
                 * case 4: start at the start of the first body block, copy
                 * everything except the last block. */
                walk_start = LARGE_TITLE_CHUNK_DATA_SZ - key_size;

                /* calculate the number of bytes left in the last block */
                walk_end = value_size;
                if (walk_end >= (LARGE_TITLE_CHUNK_DATA_SZ - key_size)) {
                    walk_end -= (LARGE_TITLE_CHUNK_DATA_SZ - key_size);
                }

                walk_end %= LARGE_BODY_CHUNK_DATA_SZ;
                if (walk_end == 0) {
                    /* we fit exactly into the body chunks.  so to exempt the
                     * last chunk, chop off LARGE_BODY_CHUNK_DATA_SZ. */
                    walk_end = LARGE_BODY_CHUNK_DATA_SZ;
                }

                /* so walk over all bytes except for the leftover bytes. */
                walk_end = value_size - walk_end;
                walk_end --;            /* length to offset conversion... */

                clear_item(it);

                cntr = 0;

/*                 printf(" value size = %ld\n", value_size); */
/*                 printf(" walking range %ld-%ld\n", walk_start, */
/*                        walk_end); */
#define LARGE_MULTI_CHUNK_ITEM_TEST_APPLIER(_it, _ptr, _bytes)         \
                /* printf("   _ptr = %p, _bytes = %ld\n", _ptr, _bytes); */ \
                    TASSERT(_it == it);                                 \
                    memset(_ptr, 0x5a, _bytes);                         \
                    TASSERT(_ptr < &it->large_title.data[0] ||          \
                            _ptr > &it->large_title.data[LARGE_TITLE_CHUNK_DATA_SZ - 1]); \
                                                                        \
                    cntr ++;

                ITEM_WALK(it, walk_start, walk_end - walk_start + 1, false, LARGE_MULTI_CHUNK_ITEM_TEST_APPLIER, );
#undef LARGE_MULTI_CHUNK_ITEM_TEST_APPLIER

                TASSERT(cntr == num_chunks - 2);

                copy = item_copy_out(it);

                TASSERT(byte_compare(copy, 0, key_size - 1, 0) == 0);
                TASSERT(byte_compare(copy, key_size, key_size + walk_start - 1, 0) == 0);
                TASSERT(byte_compare(copy, key_size + walk_start, key_size + walk_end, 0x5a) == 0);
                TASSERT(byte_compare(copy, key_size + walk_end + 1, item_size - 1, 0) == 0);

                free(copy);

                do_item_deref(it);
            }
        }
    }

    V_PRINTF(2, "\n");

    return 0;
}


tester_info_t tests[] = {
  { small_single_chunk_item_test, 1 },
  { large_single_chunk_item_test, 0 },
  { beyond_small_single_chunk_item_test, 1 },
  { beyond_large_single_chunk_item_test, 1 },
  { small_multi_chunk_item_test, 1 },
  { large_multi_chunk_item_test, 1 },
};


#include "main.h"
