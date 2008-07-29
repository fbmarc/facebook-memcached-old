/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "flat_storage.h"
#include "flat_storage_support.h"


static int
is_large_chunk_test(int verbose) {
    int i;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    /* find the transition from small to large chunks */
    for (i = 0;
         ! is_large_chunk(0, i);
         i ++) {
        ;
    }

    /* i is supposedly the first value that requires large chunks.  it should
     * be equal to (1 * SMALL_TITLE_CHUNK's data field) +
     * ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 2) * SMALL_BODY_CHUNK's data field) +
     * 1 */
    TASSERT( ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
             ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 2) * sizeof( ((small_body_chunk_t*) 0)->data ) ) +
             1 == i );

    return 0;
}


static int
chunks_needed_test(int verbose) {
    int i;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    /* in this test, use a key size of 0 to ensure we don't trip the requirement
     * that the entire key fit in the first chunk */

    /* test some boundary conditions... 1->2 and 2->3 chunks. */

    /* start with small chunks.... */
    for (i = 1;
         chunks_needed(0, i) == 1;
         i ++) {
        ;
    }

    /* should be requesting 2 chunks */
    TASSERT(chunks_needed(0, i) == 2);

    /* is the data size what we expected? */
    TASSERT(sizeof( ((small_title_chunk_t*) 0)->data ) + 1 == i);

    /* trip the 2->3 chunks transition */
    for (;
         chunks_needed(0, i) == 2;
         i++ ) {
        ;
    }

    /* should be requesting 3 chunks */
    TASSERT(chunks_needed(0, i) == 3);

    /* is the data size what we expected? */
    TASSERT( sizeof( ((small_title_chunk_t*) 0)->data ) +
             sizeof( ((small_body_chunk_t*) 0)->data ) + 1 == i );

    /* large chunks.... */
    for (i = 1;
         ! is_large_chunk(0, i);
         i ++) {
        ;
    }

    /* i should be the minimal data size to require a large chunk */
    TASSERT(chunks_needed(0, i) == 1);

    for (;
         chunks_needed(0, i) == 1;
         i ++) {
        ;
    }

    /* should be requesting 2 chunks */
    TASSERT(chunks_needed(0, i) == 2);

    /* is the data size what we expected? */
    TASSERT(sizeof( ((large_title_chunk_t*) 0)->data ) + 1 == i);

    /* trip the 2->3 chunks transition */
    for (;
         chunks_needed(0, i) == 2;
         i++ ) {
        ;
    }

    /* should be requesting 3 chunks */
    TASSERT(chunks_needed(0, i) == 3);

    /* is the data size what we expected? */
    TASSERT(sizeof( ((large_title_chunk_t*) 0)->data ) +
            sizeof( ((large_body_chunk_t*) 0)->data ) + 1 == i);

    /* now some random tests */
    for (i = 0;
         i < CHUNKS_NEEDED_RANDOM_TESTS;
         i ++) {
        ssize_t datasz;
        size_t chunks;

        datasz = (random() % MAX_ITEM_SIZE) + 1;
        chunks = chunks_needed(0, datasz);

        if (is_large_chunk(0, datasz)) {
            TASSERT(datasz <= (sizeof( ((large_title_chunk_t*) 0)->data ) +
                               (chunks - 1) * sizeof( ((large_body_chunk_t*) 0)->data )));
        } else {
            TASSERT(datasz <= (sizeof( ((small_title_chunk_t*) 0)->data ) +
                               (chunks - 1) * sizeof( ((small_body_chunk_t*) 0)->data )));
        }
    }

    return 0;
}


static int
slackspace_test(int verbose) {
    unsigned ksize, vsize;
    int ksizes[] = { sizeof( ((small_title_chunk_t*) 0)->data ) - 1,
                     sizeof( ((small_title_chunk_t*) 0)->data ),
                     sizeof( ((small_title_chunk_t*) 0)->data ) + 1,
                     ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
                     ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 2) * sizeof( ((small_body_chunk_t*) 0)->data ) ) - 1,
                     ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
                     ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 2) * sizeof( ((small_body_chunk_t*) 0)->data ) ),
                     ( sizeof( ((small_title_chunk_t*) 0)->data ) ) +
                     ( (SMALL_CHUNKS_PER_LARGE_CHUNK - 2) * sizeof( ((small_body_chunk_t*) 0)->data ) ),
    };

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    for (vsize = 0;
         vsize < 16 * 1024;
         vsize ++) {
        for (ksize = 0;
             ksize < (sizeof(ksizes) / sizeof(*ksizes));
             ksize ++) {
            size_t slack = slackspace(ksizes[ksize], vsize);

            if (vsize % 1024 == 0) {
                V_PRINTF(2, "\r  *  allocating object value size=%u", vsize);
                V_FLUSH(2);
            }

            if (vsize == 0 &&
                ksizes[ksize] == 0) {
                continue;
            }

            TASSERT( is_large_chunk(ksizes[ksize], vsize) == is_large_chunk(ksizes[ksize], vsize + slack) );
            TASSERT( chunks_needed(ksizes[ksize], vsize) == chunks_needed(ksizes[ksize], vsize + slack) );
        }
    }
    V_PRINTF(2, "\n");

    return 0;
}


static int
chunkptr_test(int verbose) {
    /* grab a large chunk indirectly. */
    large_chunk_t* prev_lc = fsi.flat_storage_start;
    large_chunk_t* lc = prev_lc + 1;
    large_chunk_t* next_lc = lc + 1;
    chunk_t* temp;
    int i;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    /* make sure we don't map to NULL_CHUNKPTR */
    TASSERT(get_chunkptr((chunk_t*) prev_lc) != NULL_CHUNKPTR);
    TASSERT(get_chunkptr((chunk_t*) lc) != NULL_CHUNKPTR);
    TASSERT(get_chunkptr((chunk_t*) next_lc) != NULL_CHUNKPTR);

    /* make sure that consecutive lcs don't map to each other. */
    TASSERT(get_chunkptr((chunk_t*) prev_lc) != get_chunkptr((chunk_t*) lc));
    TASSERT(get_chunkptr((chunk_t*) lc) != get_chunkptr((chunk_t*) next_lc));

    /* can we do a roundtrip on the function? */
    temp = get_chunk_address(get_chunkptr((chunk_t*) prev_lc));
    TASSERT(prev_lc == &temp->lc);

    temp = get_chunk_address(get_chunkptr((chunk_t*) lc));
    TASSERT(lc == &temp->lc);

    temp = get_chunk_address(get_chunkptr((chunk_t*) next_lc));
    TASSERT(next_lc == &temp->lc);

    /* iterate through the small chunks in LC. */
    for (i = 0;
         i < SMALL_CHUNKS_PER_LARGE_CHUNK;
         i ++) {
        small_chunk_t* sc = &lc->lc_broken.lbc[i];
        chunkptr_t ptr = get_chunkptr((chunk_t*) sc);

        /* if we're not the first chunk and we yield the same chunkptr as the
         * parent chunk, that's bad. */
        TASSERT(! ((i == 0) ^
                   (ptr == get_chunkptr((chunk_t*) lc))));

        /* we should never match the chunkptr as the prev chunk. */
        TASSERT(ptr != get_chunkptr((chunk_t*) prev_lc));
        /* we should never match the chunkptr as the next chunk. */
        TASSERT(ptr != get_chunkptr((chunk_t*) next_lc));

        /* ensure we can do roundtrips here too. */
        temp = get_chunk_address(get_chunkptr((chunk_t*) sc));
        TASSERT(sc == &temp->sc);
    }

    return 0;
}


static int
get_parent_chunk_test(int verbose) {
    /* grab a large chunk indirectly. */
    large_chunk_t* prev_lc = fsi.flat_storage_start;
    large_chunk_t* lc = prev_lc + 1;
    int i;

    V_LPRINTF(1, "%s\n", __FUNCTION__);

    /* we have to hack in the proper flags because we're not actually calling
     * the proper routines to allocate and break the large chunk. */
    if (verbose == 2) {
        printf("  *  before lc->flags = %d\n", lc->flags);
    }

    lc->flags |= (LARGE_CHUNK_USED | LARGE_CHUNK_BROKEN);
    lc->flags &= ~(LARGE_CHUNK_FREE);

    if (verbose == 2) {
        printf("  *  after lc->flags = %d\n", lc->flags);
    }

    for (i = 0;
         i < SMALL_CHUNKS_PER_LARGE_CHUNK;
         i ++) {
        small_chunk_t* sc = &lc->lc_broken.lbc[i];
        large_chunk_t* pc = get_parent_chunk(sc);

        TASSERT(pc == lc);
    }

    return 0;
}


tester_info_t tests[] = { {is_large_chunk_test, 1},
                          {chunks_needed_test, 1},
                          {slackspace_test, 1},
                          {chunkptr_test, 1},
                          {get_parent_chunk_test, 1},
};


#include "main.h"
