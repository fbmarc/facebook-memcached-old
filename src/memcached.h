/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/* $Id$ */

#if !defined(_memcached_h_)
#define _memcached_h_

#include "generic.h"

#include <netinet/in.h>
#include <sys/time.h>
#include <sys/socket.h>

#include <event.h>

/**
 * initial buffer sizes.
 */
#define DATA_BUFFER_SIZE 2048
#define BP_HDR_POOL_INIT_SIZE 4096
#define UDP_READ_BUFFER_SIZE 65536
#define UDP_MAX_PAYLOAD_SIZE 1400
#define MAX_SENDBUF_SIZE (256 * 1024 * 1024)

/** Initial size of list of items being returned by "get". */
#define ITEM_LIST_INITIAL 200

/** Initial size of the sendmsg() scatter/gather array. */
#define IOV_LIST_INITIAL 400

/** Initial number of sendmsg() argument structures to allocate. */
#define MSG_LIST_INITIAL 10

/** High water marks for buffer shrinking */
#define READ_BUFFER_HIGHWAT 8192
#define WRITE_BUFFER_HIGHWAT 8192
#define ITEM_LIST_HIGHWAT 400
#define IOV_LIST_HIGHWAT 600
#define MSG_LIST_HIGHWAT 100

/** other useful constants. */
#define BUFFER_ALIGNMENT (sizeof(uint32_t))
#define KEY_MAX_LENGTH 255
#define MAX_ITEM_SIZE  (1024 * 1024)
#define UDP_HEADER_SIZE 8

/* number of virtual buckets for a managed instance */
#define MAX_BUCKETS 32768

/*
 * We only reposition items in the LRU queue if they haven't been repositioned
 * in this many seconds. That saves us from churning on frequently-accessed
 * items.
 */
#define ITEM_UPDATE_INTERVAL 60


/**
 * the following are the maximum sizes of the responses for various stat
 * commands.
 */
#define ITEM_CACHEDUMP_LIMIT   (2 * 1024 * 1024)
#define ITEM_STATS_SIZES       (2 * 1024 * 1024)


/**
 * forward declare structures.
 */
typedef struct stats_s       stats_t;
typedef struct settings_s    settings_t;
typedef struct conn_s        conn;


/**
 * define types that don't rely on other modules.
 */

typedef enum conn_states_e {
    conn_listening,  /** the socket which listens for connections */
    conn_read,       /** reading in a command line */
    conn_write,      /** writing out a simple response */
    conn_nread,      /** reading in a fixed number of bytes */
    conn_swallow,    /** swallowing unnecessary bytes w/o storing */
    conn_closing,    /** closing this connection */
    conn_mwrite,     /** writing out many items sequentially */

    conn_bp_header_size_unknown,        /** waiting for enough data to determine
                                            the size of the header. */
    conn_bp_header_size_known,          /** header size known.  this means we've
                                            at least read in the command byte.*/
    conn_bp_waiting_for_key,            /** received the header, waiting for the
                                            key. */
    conn_bp_waiting_for_value,          /** received the key, waiting for the
                                            value. */
    conn_bp_waiting_for_string,         /** received the header, waiting for the
                                            string. */
    conn_bp_process,                    /** process the request. */
    conn_bp_writing,                    /** in the process of writing the
                                            output. */
    conn_bp_error,
} conn_states_t;


enum nread_e {
    NREAD_ADD     = 1,
    NREAD_SET     = 2,
    NREAD_REPLACE = 3,
};


enum transmit_sts_e {
    TRANSMIT_COMPLETE   = 0,
    TRANSMIT_INCOMPLETE = 1,
    TRANSMIT_SOFT_ERROR = 2,
    TRANSMIT_HARD_ERROR = 3,
};


struct stats_s {
    unsigned int  curr_items;
    unsigned int  total_items;
    uint64_t      item_storage_allocated;
    uint64_t      item_total_size;
    unsigned int  curr_conns;
    unsigned int  total_conns;
    unsigned int  conn_structs;
    uint64_t      get_cmds;
    uint64_t      set_cmds;
    uint64_t      get_hits;
    uint64_t      get_misses;
    uint64_t      arith_cmds;
    uint64_t      arith_hits;
    uint64_t      evictions;
    time_t        started;          /* when the process was started */
    uint64_t      bytes_read;
    uint64_t      bytes_written;

    uint64_t      get_bytes;
    uint64_t      byte_seconds;

#define MEMORY_POOL(pool_enum, pool_counter, pool_string) uint64_t pool_counter;
#include "memory_pool_classes.h"

    struct {
#define MEMORY_POOL(pool_enum, pool_counter, pool_string) uint64_t pool_counter;
#include "memory_pool_classes.h"
    } mp_bytecount_errors_realloc_split;

    struct {
#define MEMORY_POOL(pool_enum, pool_counter, pool_string) uint64_t pool_counter;
#include "memory_pool_classes.h"
    } mp_bytecount_errors_free_split;

    uint64_t      mp_blk_errors;
    uint64_t      mp_bytecount_errors;
    uint64_t      mp_pool_errors;
};


#define MAX_VERBOSITY_LEVEL 2
struct settings_s {
    size_t maxbytes;
    int maxconns;
    int port;
    int udpport;
    int binary_port;
    int binary_udpport;
    struct in_addr interf;
    int verbose;
    rel_time_t oldest_live; /* ignore existing items older than this */
    bool managed;          /* if 1, a tracker manages virtual buckets */
    int evict_to_free;
    char *socketpath;   /* path to unix socket if using local socket */
    double factor;          /* chunk size growth factor */
    int chunk_size;
    int num_threads;        /* number of libevent threads to run */
    char prefix_delimiter;  /* character that marks a key prefix (for stats) */
    int detail_enabled;     /* nonzero if we're collecting detailed stats */
    int reqs_per_event;     /* Maximum number of requests to process on each
                               io-event. */
    size_t max_conn_buffer_bytes;       /* high-water mark for memory taken by
                                         * connection buffers. */
};


/**
 * bring in other modules that we depend on for structure definitions.
 */
#include "binary_protocol.h"
#include "binary_sm.h"
#include "items.h"


/**
 * define types that rely on other modules.
 */

struct conn_s {
    int    sfd;
    int    ufd;     /** udp fd */
    int    xfd;     /** transmit fd */
    conn_states_t state;
    struct event event;
    short  ev_flags;
    short  which;   /** which events were just triggered */

    char   *rbuf;   /** buffer to read commands into */
    char   *rcurr;  /** but if we parsed some already, this is where we stopped */
    int    rsize;   /** total allocated size of rbuf */
    int    rbytes;  /** how much data, starting from rcur, do we have unparsed */

    char   *wbuf;
    char   *wcurr;
    int    wsize;
    int    wbytes;
    conn_states_t write_and_go; /** which state to go into after finishing current write */
    void   *write_and_free; /** free this memory after finishing writing */

    /* data for the nread state */

    struct iovec* riov;        /* read iov */
    size_t riov_size;          /* number of read iovs allocated */
    size_t riov_curr;          /* current read iov being sent */
    size_t riov_left;          /* number of read iovs left to send */

    /**
     * item is used to hold an item structure created after reading the command
     * line of set/add/replace commands, but before we finished reading the actual
     * data. The data is read into ITEM_data(item) to avoid extra copying.
     */

    void   *item;     /* for commands set/add/replace  */
    int    item_comm; /* which one is it: set/add/replace */
    const char *update_key;

    /* data for the swallow state */
    int    sbytes;    /* how many bytes to swallow */

    /* data for the mwrite state */
    struct iovec *iov; /* this is a pool of iov items, which gets bundled into
                        * the msgs (struct msghdr). */
    int    iovsize;   /* number of elements allocated in iov[] */
    int    iovused;   /* number of elements used in iov[] */

    struct msghdr *msglist;
    int    msgsize;   /* number of elements allocated in msglist[] */
    int    msgused;   /* number of elements used in msglist[] */
    int    msgcurr;   /* element in msglist[] being transmitted now */
    int    msgbytes;  /* number of bytes in current msg */

    item   **ilist;   /* list of items to write out */
    int    isize;
    item   **icurr;
    int    ileft;

    char   crlf[2];   /* used to receive cr-lfs from the ascii protocol. */

    /* data for UDP clients */
    bool   udp;       /* is this is a UDP "connection" */
    int    request_id; /* Incoming UDP request ID, if this is a UDP "connection" */
    struct sockaddr request_addr; /* Who sent the most recent request */
    socklen_t request_addr_size;
    unsigned char *hdrbuf; /* udp packet headers */
    int    hdrsize;   /* number of headers' worth of space is allocated */

    bool   binary;    /* are we in binary mode */
    int    bucket;    /* bucket number for the next command, if running as
                         a managed instance. -1 (_not_ 0) means invalid. */
    int    gen;       /* generation requested for the bucket */

    /* used to process binary protocol messages */
    bp_cmd_info_t bp_info;

    union {
        empty_req_t      empty_req;
        key_req_t        key_req;
        key_value_req_t  key_value_req;
        key_number_req_t key_number_req;
        number_req_t     number_req;
        string_req_t     string_req;
    } u;
    bp_hdr_pool_t* bp_hdr_pool;

    char*  bp_key;
    char*  bp_string;
};

extern stats_t stats;
extern settings_t settings;

/* current time of day (updated periodically) */
extern volatile rel_time_t current_time;

/* temporary hack */
/* #define assert(x) if(!(x)) { printf("assert failure: %s\n", #x); pre_gdb(); }
   void pre_gdb (); */

/*
 * Functions
 */
conn *do_conn_from_freelist();
bool do_conn_add_to_freelist(conn* c);
int  do_defer_delete(item *item, time_t exptime);
void do_run_deferred_deletes(void);
char *do_add_delta(const char* key, const size_t nkey, const int incr, const unsigned int delta,
                   char *buf, uint32_t* res_val, const struct in_addr addr);
int do_store_item(item *item, int comm, const char* key);
conn* conn_new(const int sfd, const int init_state, const int event_flags, const int read_buffer_size,
                 const bool is_udp, const bool is_binary,
                 const struct sockaddr* const addr, const socklen_t addrlen,
                 struct event_base *base);
void conn_cleanup(conn* c);
void conn_close(conn* c);
void conn_shrink(conn* c);
void accept_new_conns(const bool do_accept, const bool is_binary);
bool update_event(conn* c, const int new_flags);
int add_iov(conn* c, const void *buf, int len, bool is_start);
int add_msghdr(conn* c);
rel_time_t realtime(const time_t exptime);
int build_udp_headers(conn* c);
size_t append_to_buffer(char* const buffer_start,
                        const size_t buffer_size,
                        const size_t buffer_off,
                        const size_t reserved,
                        const char* fmt,
                        ...);
extern int try_read_network(conn *c);
extern int try_read_udp(conn *c);
extern int transmit(conn *c);

/*
 * In multithreaded mode, we wrap certain functions with lock management and
 * replace the logic of some other functions. All wrapped functions have
 * "mt_" and "do_" variants. In multithreaded mode, the plain version of a
 * function is #define-d to the "mt_" variant, which often just grabs a
 * lock and calls the "do_" function. In singlethreaded mode, the "do_"
 * function is called directly.
 *
 * Functions such as the libevent-related calls that need to do cross-thread
 * communication in multithreaded mode (rather than actually doing the work
 * in the current thread) are called via "dispatch_" frontends, which are
 * also #define-d to directly call the underlying code in singlethreaded mode.
 */
#ifdef USE_THREADS

void thread_init(int nthreads, struct event_base *main_base);
int  dispatch_event_add(int thread, conn* c);
void dispatch_conn_new(int sfd, int init_state, int event_flags,
                       const int read_buffer_size,
                       const bool is_udp, const bool is_binary,
                       const struct sockaddr* addr, socklen_t addrlen);

/* Lock wrappers for cache functions that are called from main loop. */
char *mt_add_delta(const char* key, const size_t nkey, const int incr, const unsigned int delta,
                   char *buf, uint32_t *res, const struct in_addr addr);
size_t mt_append_thread_stats(char* const buf, const size_t size, const size_t offset, const size_t reserved);
int   mt_assoc_expire_regex(char *pattern);
void  mt_assoc_move_next_bucket(void);
conn* mt_conn_from_freelist(void);
bool  mt_conn_add_to_freelist(conn* c);
int   mt_defer_delete(item *it, time_t exptime);
int   mt_is_listen_thread(void);
item *mt_item_alloc(char *key, size_t nkey, int flags, rel_time_t exptime, int nbytes, const struct in_addr addr);
char *mt_item_cachedump(const unsigned int slabs_clsid, const unsigned int limit, unsigned int *bytes);
void  mt_item_flush_expired(void);
item *mt_item_get_notedeleted(const char *key, const size_t nkey, bool *delete_locked);
void  mt_item_deref(item *it);
char *mt_item_stats(int *bytes);
char *mt_item_stats_sizes(int *bytes);
void  mt_item_unlink(item *it, long flags, const char* key);
void  mt_item_update(item *it);
void  mt_run_deferred_deletes(void);
void *mt_slabs_alloc(size_t size);
void  mt_slabs_free(void *ptr, size_t size);
int   mt_slabs_reassign(unsigned char srcid, unsigned char dstid);
void  mt_slabs_rebalance();
char *mt_slabs_stats(int *buflen);
void  mt_stats_lock(void);
void  mt_stats_unlock(void);
int   mt_store_item(item *item, int comm, const char* key);


# define add_delta                   mt_add_delta
# define alloc_conn_buffer           mt_alloc_conn_buffer
# define append_thread_stats         mt_append_thread_stats
# define assoc_expire_regex          mt_assoc_expire_regex
# define assoc_move_next_bucket      mt_assoc_move_next_bucket
# define conn_from_freelist          mt_conn_from_freelist
# define conn_add_to_freelist        mt_conn_add_to_freelist
# define conn_buffer_reclamation     mt_conn_buffer_reclamation
# define conn_buffer_stats           mt_conn_buffer_stats
# define defer_delete                mt_defer_delete
# define flat_allocator_stats        mt_flat_allocator_stats
# define free_conn_buffer            mt_free_conn_buffer
# define is_listen_thread            mt_is_listen_thread
# define item_alloc                  mt_item_alloc
# define item_cachedump              mt_item_cachedump
# define item_flush_expired          mt_item_flush_expired
# define item_get_notedeleted        mt_item_get_notedeleted
# define item_deref                  mt_item_deref
# define item_stats                  mt_item_stats
# define item_stats_sizes            mt_item_stats_sizes
# define item_update                 mt_item_update
# define item_unlink                 mt_item_unlink
# define run_deferred_deletes        mt_run_deferred_deletes
# define slabs_alloc                 mt_slabs_alloc
# define slabs_free                  mt_slabs_free
# define slabs_reassign              mt_slabs_reassign
# define slabs_rebalance             mt_slabs_rebalance
# define slabs_stats                 mt_slabs_stats
# define store_item                  mt_store_item
# define STATS_LOCK()                mt_stats_lock()
# define STATS_UNLOCK()              mt_stats_unlock()

#else /* !USE_THREADS */

# define add_delta                   do_add_delta
# define alloc_conn_buffer           do_alloc_conn_buffer
# define append_thread_stats(b,s,o,r) o
# define assoc_expire_regex          do_assoc_expire_regex
# define assoc_move_next_bucket      do_assoc_move_next_bucket
# define conn_from_freelist          do_conn_from_freelist
# define conn_add_to_freelist        do_conn_add_to_freelist
# define conn_buffer_reclamation     do_conn_buffer_reclamation
# define conn_buffer_stats           do_conn_buffer_stats
# define defer_delete                do_defer_delete
# define dispatch_conn_new(x,y,z,a,b,c,d,e) conn_new(x,y,z,a,b,c,d,e,main_base)
# define dispatch_event_add(t,c)     event_add(&(c)->event, 0)
# define flat_allocator_stats        do_flat_allocator_stats
# define free_conn_buffer            do_free_conn_buffer
# define is_listen_thread()          1
# define item_alloc                  do_item_alloc
# define item_cachedump              do_item_cachedump
# define item_flush_expired          do_item_flush_expired
# define item_get_notedeleted        do_item_get_notedeleted
# define item_deref                  do_item_deref
# define item_replace                do_item_replace
# define item_stats                  do_item_stats
# define item_stats_sizes            do_item_stats_sizes
# define item_unlink                 do_item_unlink
# define item_update                 do_item_update
# define run_deferred_deletes        do_run_deferred_deletes
# define slabs_alloc                 do_slabs_alloc
# define slabs_free                  do_slabs_free
# define slabs_reassign              do_slabs_reassign
# define slabs_rebalance             do_slabs_rebalance
# define slabs_stats                 do_slabs_stats
# define store_item                  do_store_item
# define thread_init(x,y)            ;

# define STATS_LOCK()                /**/
# define STATS_UNLOCK()              /**/

#endif /* !USE_THREADS */

static inline struct in_addr get_request_addr(conn* c) {
    struct in_addr retval = { INADDR_NONE };

    if (c->request_addr_size != 0) {
        /* nonzero request addr size */
        if (c->request_addr.sa_family == AF_INET) {
            struct sockaddr_in* sin = (struct sockaddr_in*) &(c->request_addr);

            retval = sin->sin_addr;
        }
    }

    return retval;
}

#include "memory_pool.h"

#endif /* #if !defined(_memcached_h_) */
