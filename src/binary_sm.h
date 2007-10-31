/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#if !defined(_binary_sm_h_)
#define _binary_sm_h_

#include "memcached.h"

extern void process_binary_protocol(conn* c);
extern bp_hdr_pool_t* bp_allocate_hdr_pool(bp_hdr_pool_t* next);

#endif /* #if !defined(_binary_sm_h_) */
