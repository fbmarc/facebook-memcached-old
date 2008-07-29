/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#if !defined(_items_h_)
#define _items_h_

#include <netinet/in.h>

// bit flag for do_item_unlink.

#define UNLINK_NORMAL          0x000000000
#define UNLINK_IS_EVICT        0x000000001
#define UNLINK_IS_EXPIRED      0x000000002
#define UNLINK_MAYBE_EVICT     0x000000004 /* could be either due to eviction or
                                            * expiration.  need to check the
                                            * expiration time. */

#if defined(USE_SLAB_ALLOCATOR)
#include "slabs.h"
#include "slabs_items.h"
#endif /* #if defined(USE_SLAB_ALLOCATOR) */

#if defined(USE_FLAT_ALLOCATOR)
#include "flat_storage.h"
#endif /* #if defined(USE_FLAT_ALLOCATOR) */

/* See items.c */
extern void item_init(void);
/*@null@*/
extern void do_try_item_stamp(item* it, rel_time_t now, const struct in_addr addr);
extern item* do_item_alloc(const char *key, const size_t nkey,
                           const int flags, const rel_time_t exptime, const size_t nbytes,
                           const struct in_addr addr);
extern bool  item_size_ok(const size_t nkey, const int flags, const int nbytes);

extern int   do_item_link(item *it, const char* key);     /** may fail if transgresses limits */
extern void  do_item_unlink(item *it, long flags, const char* key);
extern void  do_item_unlink_impl(item *it, long flags, bool to_freelist);
extern void  do_item_deref(item *it);
extern void  do_item_update(item *it);   /** update LRU time to current and reposition */
extern int   do_item_replace(item *it, item *new_it, const char* key);

/*@null@*/
extern char* do_item_stats_sizes(int *bytes);
extern void  do_item_flush_expired(void);
extern item* item_get(const char *key, const size_t nkey);

extern item* do_item_get_notedeleted(const char *key, const size_t nkey, bool *delete_locked);
extern item* do_item_get_nocheck(const char *key, const size_t nkey);

/* returns true if a deleted item's delete-locked-time is over, and it
   should be removed from the namespace */
extern bool  item_delete_lock_over(item *it);

extern bool  item_need_realloc(const item* it,
                               const size_t new_nkey, const int new_flags, const size_t new_nbytes);

extern void item_memcpy_to(item* it, size_t offset, const void* src, size_t nbytes,
                           bool beyond_item_boundary);
extern void item_memcpy_from(void* dst, const item* it, size_t offset, size_t nbytes,
                             bool beyond_item_boundary);
extern int item_key_compare(const item* it, const char* key, const size_t nkey);

#endif /* #if !defined(_items_h_) */
