/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/* this is a lame implementation of assoc.  it is limited to a fixed set of keys
 * and search is *really* lame (i.e., linear).  but it suffices for testing
 * code. */

#include "generic.h"

#include <assert.h>
#include <stdlib.h>
#include "memcached.h"
#include "assoc.h"

typedef struct map_s {
    item* pointer;
    char key[KEY_MAX_LENGTH + 1];
    uint8_t klen;
    uint8_t valid;
} map_t;

static map_t lookup[MAX_KEYS];

item* assoc_find(const char* key, const size_t nkey) {
    int i;

    for (i = 0; i < MAX_KEYS; i ++) {
        if (lookup[i].valid &&
            lookup[i].klen == nkey &&
            memcmp(lookup[i].key, key, nkey) == 0) {
            assert(ITEM_nkey(lookup[i].pointer) == nkey &&
                   memcmp(ITEM_key(lookup[i].pointer), key, nkey) == 0);
            return lookup[i].pointer;
        }
    }
    return NULL;
}


void assoc_delete(const char* key, const size_t nkey, item_ptr_t to_be_deleted) {
    int i;
    for (i = 0; i < MAX_KEYS; i ++) {
        if (lookup[i].valid &&
            nkey == lookup[i].klen &&
            memcmp(lookup[i].key, key, nkey) == 0) {
            lookup[i].valid = 0;
        }
    }
}


int assoc_insert(item* it) {
    int valid_space = MAX_KEYS;
    int i;

    for (i = 0; i < MAX_KEYS; i ++) {
        if (lookup[i].valid == 0) {
            valid_space = i;
        }
        assert(lookup[i].valid == 0 ||
               ITEM_nkey(it) != lookup[i].klen ||
               memcmp(lookup[i].key, ITEM_key(it), ITEM_nkey(it)) != 0);
    }

    if (valid_space != MAX_KEYS) {
        memcpy(lookup[valid_space].key, ITEM_key(it), ITEM_nkey(it));
        lookup[valid_space].klen = ITEM_nkey(it);
        lookup[valid_space].valid = 1;
        lookup[valid_space].pointer = it;
    }

    return 1;
}


item* assoc_update(item* it) {
    item* old_item;
    int i;

    for (i = 0; i < MAX_KEYS; i ++) {
        if (lookup[i].valid &&
            ITEM_nkey(it) == lookup[i].klen &&
            memcmp(lookup[i].key, ITEM_key(it), ITEM_nkey(it)) == 0) {
            old_item = lookup[i].pointer;
            lookup[i].pointer = it;
            return old_item;
        }
    }

    abort();
}
