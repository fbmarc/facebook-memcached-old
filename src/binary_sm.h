/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#if !defined(_binary_sm_h_)
#define _binary_sm_h_

/**
 * define types that don't rely on other modules.
 */
typedef struct bp_cmd_info_s bp_cmd_info_t;
typedef struct bp_hdr_pool_s bp_hdr_pool_t;


struct bp_cmd_info_s {
    size_t header_size;
    char   has_key;
    char   has_value;
    char   has_string;
};


struct bp_hdr_pool_s {
    char*  ptr;
    size_t bytes_free;
    bp_hdr_pool_t* next;
};


#include "memcached.h"

extern void process_binary_protocol(conn_t* c);
extern bp_hdr_pool_t* bp_allocate_hdr_pool(bp_hdr_pool_t* next);

#endif /* #if !defined(_binary_sm_h_) */
