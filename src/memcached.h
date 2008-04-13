/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/* $Id$ */

#if !defined(_memcached_h_)
#define _memcached_h_

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <event.h>

#ifdef HAVE_MALLOC_H
/* OpenBSD has a malloc.h, but warns to use stdlib.h instead */
#ifndef __OpenBSD__
#include <malloc.h>
#endif
#endif

#include "binary_protocol.h"

#define DATA_BUFFER_SIZE 2048
#define BP_HDR_POOL_INIT_SIZE 4096
#define BUFFER_ALIGNMENT (sizeof(uint32_t))
#define KEY_MAX_LENGTH 255
#define UDP_READ_BUFFER_SIZE 65536
#define UDP_MAX_PAYLOAD_SIZE 1400
#define UDP_HEADER_SIZE 8
#define MAX_SENDBUF_SIZE (256 * 1024 * 1024)

/** Initial size of list of items being returned by "get". */
#define ITEM_LIST_INITIAL 200

/** Initial size of the sendmsg() scatter/gather array. */
#define IOV_LIST_INITIAL 400

/** Initial number of sendmsg() argument structures to allocate. */
#define MSG_LIST_INITIAL 10

/** High water marks for buffer shrinking */
#define READ_BUFFER_HIGHWAT 8192
#define ITEM_LIST_HIGHWAT 400
#define IOV_LIST_HIGHWAT 600
#define MSG_LIST_HIGHWAT 100

#define TRANSMIT_COMPLETE   0
#define TRANSMIT_INCOMPLETE 1
#define TRANSMIT_SOFT_ERROR 2
#define TRANSMIT_HARD_ERROR 3

/* Get a consistent bool type */
#if HAVE_STDBOOL_H
# include <stdbool.h>
#else
  typedef enum {false = 0, true = 1} bool;
#endif

#if HAVE_STDINT_H
# include <stdint.h>
#else
 typedef unsigned char             uint8_t;
#endif

/* unistd.h is here */
#if HAVE_UNISTD_H
# include <unistd.h>
#endif

#if __WORDSIZE == 64
#define PRINTF_INT64_MODIFIER "l"
#else
#define PRINTF_INT64_MODIFIER "ll"
#endif

/** Time relative to server start. Smaller than time_t on 64-bit systems. */
typedef unsigned int rel_time_t;

struct stats {
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
    uint64_t      evictions;
    time_t        started;          /* when the process was started */
    uint64_t      bytes_read;
    uint64_t      bytes_written;
};

#define MAX_VERBOSITY_LEVEL 2

struct settings {
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
};

extern struct stats stats;
extern struct settings settings;

#define ITEM_LINKED 1
#define ITEM_DELETED 2

/* temp */
#define ITEM_SLABBED 4
#define ITEM_VISITED 8  /* cache hit */
#define ITEM_HAS_IP_ADDRESS 0x10

typedef struct _stritem {
    struct _stritem *next;
    struct _stritem *prev;
    struct _stritem *h_next;    /* hash chain next */
    rel_time_t      time;       /* least recent access */
    rel_time_t      exptime;    /* expire time */
    int             nbytes;     /* size of data */
    unsigned short  refcount;
    uint8_t         nsuffix;    /* length of flags-and-length string */
    uint8_t         it_flags;   /* ITEM_* above */
    uint8_t         slabs_clsid;/* which slab class we're in */
    uint8_t         nkey;       /* key length, w/terminating null and padding */
    char            end;        /* this must be the last field in the
                                   structure. */
    /* then null-terminated key */
    /* then " flags length\r\n" (no terminating null) */
    /* then data with terminating \r\n (no terminating null; it's binary!) */
} item;

#define ITEM_key(item) (&((item)->end))
#define stritem_length ((intptr_t) &(((item*) 0)->end))

/* warning: don't use these macros with a function, as it evals its arg twice */
#define ITEM_suffix(item) (&((item)->end) + (item)->nkey + 1)
#define ITEM_data(item) (&((item)->end) + (item)->nkey + 1 + (item)->nsuffix)
#define ITEM_ntotal(item) (stritem_length + (item)->nkey + 1 + (item)->nsuffix + (item)->nbytes)

typedef enum conn_states_s {
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

/** memcache response types. */
typedef enum mcc_res_e {
  mcc_res_unknown = 0,
  mcc_res_deleted = 1,
  mcc_res_found = 2,
  mcc_res_notfound = 3,
  mcc_res_notstored = 4,
  mcc_res_ok = 5,
  mcc_res_stored = 6,
  mcc_res_aborted = 7,
  mcc_res_local_error = 8,
  mcc_res_ooo = 9,
  mcc_res_remote_error = 10,
  mcc_res_timeout = 11,
  mcc_res_waiting = 12
} mcc_res_t;

#define NREAD_ADD 1
#define NREAD_SET 2
#define NREAD_REPLACE 3

typedef struct bp_cmd_info_s {
    size_t header_size;
    char   has_key;
    char   has_value;
    char   has_string;
} bp_cmd_info_t;

typedef struct bp_hdr_pool_s {
    char*  ptr;
    size_t bytes_free;
    struct bp_hdr_pool_s* next;
} bp_hdr_pool_t;

typedef struct {
    int    sfd;
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

    char   *ritem;  /** when we read in an item's value, it goes here */
    int    rlbytes; /** remaining bytes to read while in nread state. */

    /**
     * item is used to hold an item structure created after reading the command
     * line of set/add/replace commands, but before we finished reading the actual
     * data. The data is read into ITEM_data(item) to avoid extra copying.
     */

    void   *item;     /* for commands set/add/replace  */
    int    item_comm; /* which one is it: set/add/replace */

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
} conn;

/* number of virtual buckets for a managed instance */
#define MAX_BUCKETS 32768

/* current time of day (updated periodically) */
extern volatile rel_time_t current_time;

/* temporary hack */
/* #define assert(x) if(!(x)) { printf("assert failure: %s\n", #x); pre_gdb(); }
   void pre_gdb (); */

/*
 * Functions
 */
conn *do_conn_from_freelist();
bool do_conn_add_to_freelist(conn *c);
int  do_defer_delete(item *item, time_t exptime);
void do_run_deferred_deletes(void);
char *do_add_delta(item *item, int incr, const unsigned int delta, char *buf, uint32_t* res_val, const struct in_addr addr);
int do_store_item(item *item, int comm);
conn *conn_new(const int sfd, const int init_state, const int event_flags, const int read_buffer_size,
               const bool is_udp, const bool is_binary,
               const struct sockaddr* const addr, const socklen_t addrlen,
               struct event_base *base);
void conn_cleanup(conn *c);
void conn_close(conn *c);
void accept_new_conns(const bool do_accept, const bool is_binary);
bool update_event(conn *c, const int new_flags);
int add_iov(conn *c, const void *buf, int len, bool is_start);
int add_msghdr(conn *c);
rel_time_t realtime(const time_t exptime);
int build_udp_headers(conn *c);
size_t append_to_buffer(char* const buffer_start,
                        const size_t buffer_size,
                        const size_t buffer_off,
                        const size_t reserved,
                        const char* fmt,
                        ...);
extern int try_read_network(conn *c);
extern int try_read_udp(conn *c);
extern int transmit(conn *c);

#include "stats.h"
#include "slabs.h"
#include "assoc.h"
#include "items.h"

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
int  dispatch_event_add(int thread, conn *c);
void dispatch_conn_new(int sfd, int init_state, int event_flags,
                       const int read_buffer_size,
                       const bool is_udp, const bool is_binary,
                       const struct sockaddr* addr, socklen_t addrlen);

/* Lock wrappers for cache functions that are called from main loop. */
char *mt_add_delta(item *item, const int incr, const unsigned int delta, char *buf, uint32_t *res_val, const struct in_addr addr);
size_t mt_append_thread_stats(char* const buf, const size_t size, const size_t offset, const size_t reserved);
int   mt_assoc_expire_regex(char *pattern);
void  mt_assoc_move_next_bucket(void);
conn *mt_conn_from_freelist(void);
bool  mt_conn_add_to_freelist(conn *c);
int   mt_defer_delete(item *it, time_t exptime);
int   mt_is_listen_thread(void);
item *mt_item_alloc(char *key, size_t nkey, int flags, rel_time_t exptime, int nbytes, const struct in_addr addr);
char *mt_item_cachedump(const unsigned int slabs_clsid, const unsigned int limit, unsigned int *bytes);
void  mt_item_flush_expired(void);
item *mt_item_get_notedeleted(const char *key, const size_t nkey, bool *delete_locked);
int   mt_item_link(item *it);
void  mt_item_remove(item *it);
int   mt_item_replace(item *it, item *new_it);
char *mt_item_stats(int *bytes);
char *mt_item_stats_sizes(int *bytes);
void  mt_item_unlink(item *it, long flags);
void  mt_item_update(item *it);
void  mt_run_deferred_deletes(void);
void *mt_slabs_alloc(size_t size);
void  mt_slabs_free(void *ptr, size_t size);
int   mt_slabs_reassign(unsigned char srcid, unsigned char dstid);
void  mt_slabs_rebalance();
char *mt_slabs_stats(int *buflen);
void  mt_stats_lock(void);
void  mt_stats_unlock(void);
int   mt_store_item(item *item, int comm);

# define add_delta(x,y,z,a,b,c)      mt_add_delta(x,y,z,a,b,c)
# define append_thread_stats(b,s,o,r)  mt_append_thread_stats(b,s,o,r)
# define assoc_expire_regex(x)       mt_assoc_expire_regex(x)
# define assoc_move_next_bucket()    mt_assoc_move_next_bucket()
# define conn_from_freelist()        mt_conn_from_freelist()
# define conn_add_to_freelist(x)     mt_conn_add_to_freelist(x)
# define defer_delete(x,y)           mt_defer_delete(x,y)
# define is_listen_thread()          mt_is_listen_thread()
# define item_alloc(x,y,z,a,b,c)     mt_item_alloc(x,y,z,a,b,c)
# define item_cachedump(x,y,z)       mt_item_cachedump(x,y,z)
# define item_flush_expired()        mt_item_flush_expired()
# define item_get_notedeleted(x,y,z) mt_item_get_notedeleted(x,y,z)
# define item_link(x)                mt_item_link(x)
# define item_remove(x)              mt_item_remove(x)
# define item_replace(x,y)           mt_item_replace(x,y)
# define item_stats(x)               mt_item_stats(x)
# define item_stats_sizes(x)         mt_item_stats_sizes(x)
# define item_update(x)              mt_item_update(x)
# define item_unlink(x,y)            mt_item_unlink(x,y)
# define run_deferred_deletes()      mt_run_deferred_deletes()
# define slabs_alloc(x)              mt_slabs_alloc(x)
# define slabs_free(x,y)             mt_slabs_free(x,y)
# define slabs_reassign(x,y)         mt_slabs_reassign(x,y)
# define slabs_rebalance()           mt_slabs_rebalance()
# define slabs_stats(x)              mt_slabs_stats(x)
# define store_item(x,y)             mt_store_item(x,y)

# define STATS_LOCK()                mt_stats_lock()
# define STATS_UNLOCK()              mt_stats_unlock()

#else /* !USE_THREADS */

# define add_delta(x,y,z,a,b,c)      do_add_delta(x,y,z,a,b,c)
# define append_thread_stats(b,s,o,r) o
# define assoc_expire_regex(x)       do_assoc_expire_regex(x)
# define assoc_move_next_bucket()    do_assoc_move_next_bucket()
# define conn_from_freelist()        do_conn_from_freelist()
# define conn_add_to_freelist(x)     do_conn_add_to_freelist(x)
# define defer_delete(x,y)           do_defer_delete(x,y)
# define dispatch_conn_new(x,y,z,a,b,c,d,e) conn_new(x,y,z,a,b,c,d,e,main_base)
# define dispatch_event_add(t,c)     event_add(&(c)->event, 0)
# define is_listen_thread()          1
# define item_alloc(x,y,z,a,b,c)     do_item_alloc(x,y,z,a,b,c)
# define item_cachedump(x,y,z)       do_item_cachedump(x,y,z)
# define item_flush_expired()        do_item_flush_expired()
# define item_get_notedeleted(x,y,z) do_item_get_notedeleted(x,y,z)
# define item_link(x)                do_item_link(x)
# define item_remove(x)              do_item_remove(x)
# define item_replace(x,y)           do_item_replace(x,y)
# define item_stats(x)               do_item_stats(x)
# define item_stats_sizes(x)         do_item_stats_sizes(x)
# define item_unlink(x,y)            do_item_unlink(x,y)
# define item_update(x)              do_item_update(x)
# define run_deferred_deletes()      do_run_deferred_deletes()
# define slabs_alloc(x)              do_slabs_alloc(x)
# define slabs_free(x,y)             do_slabs_free(x,y)
# define slabs_reassign(x,y)         do_slabs_reassign(x,y)
# define slabs_rebalance()           do_slabs_rebalance()
# define slabs_stats(x)              do_slabs_stats(x)
# define store_item(x,y)             do_store_item(x,y)
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

#endif /* #if !defined(_memcached_h_) */
