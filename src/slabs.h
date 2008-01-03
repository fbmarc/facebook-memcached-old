/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#if defined(USE_SLAB_ALLOCATOR)
#if !defined(_slabs_h_)
#define _slabs_h_

/* slabs memory allocation */

/** Init the subsystem. 1st argument is the limit on no. of bytes to allocate,
    0 if no limit. 2nd argument is the growth factor; each slab will use a chunk
    size equal to the previous slab's chunk size times this factor. */
void slabs_init(const size_t limit, const double factor);


/**
 * Given object size, return id to use when allocating/freeing memory for object
 * 0 means error: can't store such a large object
 */

unsigned int slabs_clsid(const size_t size);

/** Allocate object of given length. 0 on error */ /*@null@*/
void *do_slabs_alloc(const size_t size);

/** Free previously allocated object */
void do_slabs_free(void *ptr, size_t size);

/** Fill buffer with stats */ /*@null@*/
char* do_slabs_stats(int *buflen);

/* Request some slab be moved between classes
  1 = success
   0 = fail
   -1 = tried. busy. send again shortly. */
int do_slabs_reassign(unsigned char srcid, unsigned char dstid);

void slabs_add_hit(void *it, int unique);
void slabs_add_eviction(unsigned int clsid);

/* Find the worst performed slab class to free one slab from it and 
assign it to the best performed slab class. */
void do_slabs_rebalance();

/* 0 to turn off rebalance_interval; otherwise, this number is in seconds.
 * These two functions are actually implemented in items.c.
 */
void slabs_set_rebalance_interval(int interval);
int slabs_get_rebalance_interval();
#endif /* #if !defined(_slabs_h_) */
#endif /* #if defined(USE_SLAB_ALLOCATOR) */
