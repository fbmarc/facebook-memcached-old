/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#if !defined(_slabs_items_h_)
#define _slabs_items_h_

#include "generic.h"

/* forward declare some data types. */

typedef struct _stritem item;

#include "memcached.h"

#define ITEM_LINKED 1
#define ITEM_DELETED 2

/* temp */
#define ITEM_SLABBED 4
#define ITEM_VISITED 8  /* cache hit */

struct _stritem {
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
    /* then key */
    /* then data */
};

#define stritem_length    ((intptr_t) &(((item*) 0)->end))

static inline char*          ITEM_key(item* it)      { return &(it->end); }
static inline uint8_t        ITEM_nkey(const item* it)     { return it->nkey; }
static inline int            ITEM_nbytes(const item* it)   { return it->nbytes; }
static inline size_t         ITEM_ntotal(const item* it)   { return stritem_length + it->nkey + it->nbytes; }
static inline unsigned int   ITEM_flags(const item* it)    { return it->flags; }
static inline rel_time_t     ITEM_exptime(const item* it)  { return it->exptime; }
static inline unsigned short ITEM_refcount(const item* it) { return it->refcount; }

static inline void ITEM_set_exptime(item* it, rel_time_t t) { it->exptime = t; }

static inline item*  ITEM_h_next(const item* it)                 { return it->h_next; }
static inline item** ITEM_h_next_p(item* it)               { return &it->h_next; }

static inline void   ITEM_set_h_next(item* it, item* next) { it->h_next = next; }

static inline bool ITEM_is_valid(const item* it)        { return !(it->it_flags & ITEM_SLABBED); }

static inline void ITEM_mark_deleted(item* it)    { it->it_flags |= ITEM_DELETED; }
static inline void ITEM_unmark_deleted(item* it)  { it->it_flags &= ~ITEM_DELETED; }

extern char* do_item_cachedump(const unsigned int slabs_clsid, const unsigned int limit, unsigned int *bytes);

extern char* do_item_stats(int *bytes);

extern void  item_mark_visited(item* it);

#endif /* #if !defined(_slabs_items_h_) */
