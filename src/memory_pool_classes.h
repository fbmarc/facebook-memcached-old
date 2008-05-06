#ifndef MEMORY_POOL
#define MEMORY_POOL(pool_enum, pool_counter, pool_string)
#endif

MEMORY_POOL(ASSOC_POOL, assoc_alloc, "assoc")
MEMORY_POOL(CONN_POOL, conn_alloc, "conn")
MEMORY_POOL(CONN_BUFFER_RBUF_POOL, conn_buffer_rbuf_alloc, "conn_buffer_rbuf")
MEMORY_POOL(CONN_BUFFER_WBUF_POOL, conn_buffer_wbuf_alloc, "conn_buffer_wbuf")
MEMORY_POOL(CONN_BUFFER_ILIST_POOL, conn_buffer_ilist_alloc, "conn_buffer_ilist")
MEMORY_POOL(CONN_BUFFER_IOV_POOL, conn_buffer_iov_alloc, "conn_buffer_iov")
MEMORY_POOL(CONN_BUFFER_MSGLIST_POOL, conn_buffer_msglist_alloc, "conn_buffer_msglist")
MEMORY_POOL(CONN_BUFFER_HDRBUF_POOL, conn_buffer_hdrbuf_alloc, "conn_buffer_hdrbuf")
MEMORY_POOL(CONN_BUFFER_RIOV_POOL, conn_buffer_riov_alloc, "conn_buffer_riov")
MEMORY_POOL(CONN_BUFFER_BP_KEY_POOL, conn_buffer_bp_key_alloc, "conn_buffer_bp_key")
MEMORY_POOL(CONN_BUFFER_BP_HDRPOOL_POOL, conn_buffer_bp_hdrpool_alloc, "conn_buffer_bp_hdrpool")
MEMORY_POOL(CONN_BUFFER_BP_STRING_POOL, conn_buffer_bp_string_alloc, "conn_buffer_bp_string")
MEMORY_POOL(CQ_POOL, cq_alloc, "cq")
MEMORY_POOL(DELETE_POOL, delete_alloc, "defer_delete")
MEMORY_POOL(STATS_PREFIX_POOL, stats_prefix_alloc, "prefix_stats")

#undef MEMORY_POOL
