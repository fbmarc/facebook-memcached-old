/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#if defined(USE_SLAB_ALLOCATOR)
#if !defined(_items_h_)
#define _items_h_

#include "generic.h"

#define ITEM_LINKED 1
#define ITEM_DELETED 2

/* temp */
#define ITEM_SLABBED 4
#define ITEM_VISITED 8  /* cache hit */

typedef struct _stritem {
    struct _stritem *next;
    struct _stritem *prev;
    struct _stritem *h_next;    /* hash chain next */
    rel_time_t      time;       /* least recent access */
    rel_time_t      exptime;    /* expire time */
    int             nbytes;     /* size of data */
    unsigned int    flags;      /* flags field */
    unsigned short  refcount;
    uint8_t         it_flags;   /* ITEM_* above */
    uint8_t         slabs_clsid;/* which slab class we're in */
    uint8_t         nkey;       /* key length, w/terminating null and padding */
    char            end;
    /* then null-terminated key */
    /* then data with terminating \r\n (no terminating null; it's binary!) */
} item;

#define stritem_length    ((intptr_t) &(((item*) 0)->end))

static inline char* ITEM_key(item* it)
{
    return &(it->end);
}

static inline size_t ITEM_ntotal(item* it)
{
    return stritem_length + it->nkey + 1 + it->nbytes;
}

// bit flag for do_item_unlink.

#define UNLINK_NORMAL          0x000000000
#define UNLINK_IS_EVICT        0x000000001
#define UNLINK_IS_EXPIRED      0x000000002

/* See items.c */
void item_init(void);
/*@null@*/
item *do_item_alloc(char *key, const size_t nkey, const int flags, const rel_time_t exptime, const int nbytes);
void item_free(item *it, bool to_freelist);
bool item_size_ok(const size_t nkey, const int flags, const int nbytes);

int  do_item_link(item *it);     /** may fail if transgresses limits */
void do_item_unlink(item *it, long flags);
void do_item_unlink_impl(item *it, long flags, bool to_freelist);
void do_item_remove(item *it);
void do_item_update(item *it);   /** update LRU time to current and reposition */
int  do_item_replace(item *it, item *new_it);

/*@null@*/
char *do_item_cachedump(const unsigned int slabs_clsid, const unsigned int limit, unsigned int *bytes);
char *do_item_stats(int *bytes);

/*@null@*/
char *do_item_stats_sizes(int *bytes);
void do_item_flush_expired(void);
item *item_get(const char *key, const size_t nkey);

item *do_item_get_notedeleted(const char *key, const size_t nkey, bool *delete_locked);
item *do_item_get_nocheck(const char *key, const size_t nkey);

#endif /* #if !defined(_items_h_) */
#endif /* #if defined(USE_SLAB_ALLOCATOR) */
