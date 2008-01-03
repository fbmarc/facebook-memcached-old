/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/**
 * This is the handler for binary protocol requests.  It is directly invoked
 * from event_handler(..) for connections that utilize a binary protocol.
 *
 * The handler consists of a state machine.  Please consult
 * ${memcached}/src/doc/binary_sm.dot for a reference of how the state machine
 * operates.  To generate a graphic representing the state machine, install dot
 * from http://www.graphviz.org/ and run: 
 *   dot -Tpng -o <output_png> binary_sm.dot
 *
 * Inbound messages are buffered in c->rbuf/rcurr/rsize/rbytes.
 *
 * Outbound messages are iovec'ed.  The UDP header (when using UDP) is stored in
 * c->hdrbuf and managed by build_udp_headers(..).  Binary protocol reply
 * headers are stored in c->bp_hdr_pool.  Since the binary protocol reply
 * headers are always word-aligned, this can be done very efficiently.  The
 * remaining information is sourced directly from the item storage.
 */

/* need this to get IOV_MAX on some platforms. */
#ifndef __need_IOV_MAX
#define __need_IOV_MAX
#endif

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "binary_protocol.h"
#include "items_support.h"
#include "memcached.h"

#define ALLOCATE_REPLY_HEADER(conn, type, source) allocate_reply_header(conn, sizeof(type), source)

typedef struct bp_handler_res_s {
    char stop;
    char try_buffer_read;
} bp_handler_res_t;

extern struct event_base* main_base;


static inline void bp_get_req_cmd_info(bp_cmd_t cmd, bp_cmd_info_t* info);

// prototypes for the state machine.
static inline void binary_sm(conn* c);

// prototypes for handlers of the various states in the SM.
static inline bp_handler_res_t handle_header_size_unknown(conn* c);
static inline bp_handler_res_t handle_header_size_known(conn* c);
static inline bp_handler_res_t handle_direct_receive(conn* c);
static inline bp_handler_res_t handle_process(conn* c);
static inline bp_handler_res_t handle_writing(conn* c);

// prototypes for handlers of various commands/command classes.
static void handle_echo_cmd(conn* c);
static void handle_version_cmd(conn* c);
static void handle_get_cmd(conn* c);
static void handle_update_cmd(conn* c);
static void handle_delete_cmd(conn* c);
static void handle_arith_cmd(conn* c);

static void* allocate_reply_header(conn* c, size_t size, void* req);
static void release_reply_headers(conn* c);

/* TT FIXME: these are binary protocol versions of the same functions.
 * reintegrate them with the mainline functions to avoid future divergence. */
static void bp_write_err_msg(conn* c, const char* str);
static int bp_try_read_network(conn *c);
static int bp_try_read_udp(conn *c);
static int bp_transmit(conn *c);

/**
 * when libevent tells us that a socket has data to read, we read it and process
 * it. 
 */
void process_binary_protocol(conn* c) {
    int sfd, flags;
    socklen_t addrlen;
    struct sockaddr addr;

    if (c->state == conn_listening) {
        addrlen = sizeof(addr);
        if ((sfd = accept(c->sfd, &addr, &addrlen)) == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                /* these are transient, so don't log anything */
            } else if (errno == EMFILE) {
                if (settings.verbose > 0)
                    fprintf(stderr, "Too many open connections\n");
                accept_new_conns(0, c->binary);
            } else {
                perror("accept()");
            }
            return;
        }
        if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 ||
            fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
            perror("setting O_NONBLOCK");
            close(sfd);
            return;
        }
        
        /* tcp connections mandate at least one riov to receive the key and strings. */
        assert(c->riov_size >= 1);

        dispatch_conn_new(sfd, conn_bp_header_size_unknown, EV_READ | EV_PERSIST,
                          DATA_BUFFER_SIZE, false, c->binary);
        return;
    }

    binary_sm(c);
}


bp_hdr_pool_t* bp_allocate_hdr_pool(bp_hdr_pool_t* next)
{
    long memchunk, memchunk_start;
    bp_hdr_pool_t* retval;
    
    memchunk_start = memchunk = (long) malloc(sizeof(bp_hdr_pool_t) + BP_HDR_POOL_INIT_SIZE);
    if (memchunk_start == (long) NULL) {
        return NULL;
    }
    retval = (bp_hdr_pool_t*) memchunk;
    memchunk += sizeof(bp_hdr_pool_t);
    memchunk += BUFFER_ALIGNMENT - 1;
    memchunk &= ~(BUFFER_ALIGNMENT - 1);
    retval->ptr = (char*) memchunk;
    retval->bytes_free = (memchunk_start + sizeof(bp_hdr_pool_t) + BP_HDR_POOL_INIT_SIZE) - memchunk;
    retval->next = next;

    return retval;
}


/**
 * handles the state machine.
 *
 * @param  c   the connection to process the state machine for.
 */
static inline void binary_sm(conn* c) {
    bp_handler_res_t result = {0, 0};

    while (! result.stop) {
        switch (c->state) {
            case conn_bp_header_size_unknown:
                result = handle_header_size_unknown(c);
                break;
            
            case conn_bp_header_size_known:
                result = handle_header_size_known(c);
                break;

            case conn_bp_waiting_for_key:
            case conn_bp_waiting_for_value:
            case conn_bp_waiting_for_string:
                result = handle_direct_receive(c);
                break;

            case conn_bp_process:
                result = handle_process(c);
                break;

            case conn_bp_writing:
                result = handle_writing(c);
                break;

            case conn_closing:
                if (c->udp) {
                    conn_cleanup(c);
                } else {
                    conn_close(c);
                }
                result.stop = 1;
                break;

             default:
                 assert(0);
        }

        if (result.try_buffer_read) {
            result.try_buffer_read = 0;

            if ((c->udp && 
                 bp_try_read_udp(c)) ||
                (c->udp == 0 &&
                 bp_try_read_network(c)))
                continue;

            result.stop = 1;
        }
    }
}


/**
 * given a command, return some general info about the command.  the 4 fields
 * filled out are:
 *  1) the header size of the request.
 *  2) whether the request has a key.
 *  3) whether the request has a value.
 *  4) whether the request has a string.
 *
 * @param  cmd    the command in question.
 * @param  info   a pointer to the structure filled out.
 */
static inline void bp_get_req_cmd_info(bp_cmd_t cmd, bp_cmd_info_t* info)
{
    // initialize some default values.
    info->has_key = 0;
    info->has_value = 0;
    info->has_string = 0;

    switch (cmd) {
        // these commands go as an empty_req and return as an empty_rep.
        case BP_ECHO_CMD:
        case BP_QUIT_CMD:
            info->header_size = sizeof(empty_req_t);
            break;
                        
        // these commands go as an empty_req and return as a string_rep.
        case BP_VER_CMD:
            info->header_size = sizeof(empty_req_t);
            break;
                        
        // these commands go as a key_req and return as a value_rep.
        case BP_GET_CMD:
        case BP_GETQ_CMD:
            info->header_size = sizeof(key_req_t);
            info->has_key = 1;
            break;
                        
        // these commands go as a key_value_req and return as an empty_rep.
        case BP_SET_CMD:
        case BP_ADD_CMD:
        case BP_REPLACE_CMD:
        case BP_APPEND_CMD:
                        
        case BP_SETQ_CMD:
        case BP_ADDQ_CMD:
        case BP_REPLACEQ_CMD:
        case BP_APPENDQ_CMD:
            info->header_size = sizeof(key_value_req_t);
            info->has_key = 1;
            info->has_value = 1;
            break;
                        
        // these commands go as a key_number_req and return as an empty_rep.
        case BP_DELETE_CMD:
        case BP_DELETEQ_CMD:
            info->header_size = sizeof(key_number_req_t);
            info->has_key = 1;
            break;
                        
        // these commands go as a key_number_req and return as a number_rep.
        case BP_INCR_CMD:
        case BP_DECR_CMD:
            info->header_size = sizeof(key_number_req_t);
            info->has_key = 1;
            break;
                        
        // these commands go as a number_req and return as an empty_rep.
        case BP_FLUSH_ALL_CMD:
            info->header_size = sizeof(number_req_t);
            break;
                        
        // these commands go as a string_req and return as an empty_rep.
        case BP_FLUSH_REGEX_CMD:
            info->header_size = sizeof(string_req_t);
            info->has_string = 1;
            break;
                        
        // these commands go as a string_req and return as a string_rep.
        case BP_STATS_CMD:
            info->header_size = sizeof(string_req_t);
            info->has_string = 1;
            break;
            
        default:
            assert(0);
    }
}


static inline bp_handler_res_t handle_header_size_unknown(conn* c)
{
    empty_req_t* null_empty_header;
    char* empty_header_ptr, * cmd_ptr;
    size_t bytes_needed, bytes_available;
    bp_handler_res_t retval = {0, 0};

    // calculate how many bytes we need and how many bytes we have
    // to determine if we have enough to populate the header.
    empty_header_ptr = NULL;
    null_empty_header = NULL;
    cmd_ptr = (char*) &(null_empty_header->cmd);
    bytes_needed = cmd_ptr - empty_header_ptr;
    bytes_needed += sizeof(null_empty_header->cmd);
    bytes_available = c->rbytes;

    if (bytes_available >= bytes_needed) {
        // this is safe, because the command word is only a byte.
        // if it is not, then this could be a very dangerous
        // operation on platforms that don't support unaligned
        // accesses.
        empty_req_t* basic_header = (empty_req_t*) c->rcurr;

        assert(basic_header->magic == BP_REQ_MAGIC_BYTE);
        assert(sizeof(basic_header->cmd) == 1); // this ensures
        // we're not doing
        // anything
        // profoundly stupid
        // in term of
        // word-alignment.

        bp_get_req_cmd_info(basic_header->cmd, &c->bp_info);
        c->state = conn_bp_header_size_known;
    } else {
        retval.try_buffer_read = 1;
    }
                
    return retval;
}


static inline bp_handler_res_t handle_header_size_known(conn* c)
{
    size_t bytes_needed    = c->bp_info.header_size;
    size_t bytes_available = c->rbytes;
    bp_handler_res_t retval = {0, 0};

    if (bytes_available >= bytes_needed) {
        // copy the header.  we can't use it directly from the
        // receive buffer because we cannot guarantee that the
        // buffer is word-aligned.  even if we align c->rbuf,
        // subsequent requests we receive could end up unaligned.
        memcpy(&c->u.empty_req, c->rcurr, bytes_needed);
        c->rcurr += bytes_needed;
        c->rbytes -= bytes_needed;

        if (c->bp_info.has_key == 1) {
            /*
             * if we're using UDP, the key *has* to be in the same pkt.  that
             * means we've already received it.  if we go into direct_receive,
             * we must already have the data.
             */
            if (c->udp) {
                if (c->rbytes < c->u.empty_req.keylen) {
                    bp_write_err_msg(c, "UDP requests cannot be split across datagrams");
                    return retval;
                }
            } 

            /* set up the receive. */
            c->riov[0].iov_base = c->bp_key;
            c->riov[0].iov_len = c->u.empty_req.keylen;
            c->riov_curr = 0;
            c->riov_left = 1;

            c->bp_key[c->u.empty_req.keylen] = 0;
            
            c->state = conn_bp_waiting_for_key;
        } else if (c->bp_info.has_string == 1) {
            // string commands are relatively rare, so we'll dynamically
            // allocate the memory to stuff the string into.  the upshot is that
            // we know the exact length of the string.

            size_t str_size;

            assert(c->u.empty_req.cmd == BP_FLUSH_REGEX_CMD ||
                   c->u.empty_req.cmd == BP_STATS_CMD);
            
            // NOTE: null-terminating the string!
            str_size = ntohl(c->u.string_req.body_length) - (sizeof(string_req_t) - BINARY_PROTOCOL_REQUEST_HEADER_SZ);

            c->bp_string = malloc(str_size + 1);
            if (c->bp_string == NULL) {
                // not enough memory, skip straight to the process step, which
                // should deal with this situation.
                c->state = conn_bp_process;
                return retval;
            }
            c->bp_string[str_size] = 0;
            c->riov[0].iov_base = c->bp_string;
            c->riov[0].iov_len = str_size;
            c->riov_curr = 0;
            c->riov_left = 1;

            c->state = conn_bp_waiting_for_string;
        } else {
            c->state = conn_bp_process;
        }
    } else {
        retval.try_buffer_read = 1;
    }

    return retval;
}


static inline bp_handler_res_t handle_direct_receive(conn* c)
{
    bp_handler_res_t retval = {0, 0};

    /* 
     * check if the receive buffer has any more content.  move that to the
     * destination.
     */
    if (c->rbytes > 0 &&
        c->riov_left > 0) {
        struct iovec* current_iov = &c->riov[c->riov_curr];
        size_t bytes_to_copy = (c->rbytes <= current_iov->iov_len) ? c->rbytes : current_iov->iov_len;

        memcpy(current_iov->iov_base, c->rcurr, bytes_to_copy);
        c->rcurr += bytes_to_copy;      // update receive buffer.
        c->rbytes -= bytes_to_copy;
        current_iov->iov_base += bytes_to_copy;
        current_iov->iov_len -= bytes_to_copy;

        /* are we done with the current IOV? */
        if (current_iov->iov_len == 0) {
            c->riov_curr ++;
            c->riov_left --;
        }
    }

    /* 
     * the only reason we should be here is to receive the key, which should
     * already be in the datagram. 
     */
    if (c->udp) {
        assert(c->state == conn_bp_waiting_for_key);
        assert(c->riov_left == 0);
    }

    // do we have all that we need?
    if (c->riov_left == 0) {
        // next state?
        switch (c->state) {
            case conn_bp_waiting_for_key:
                if (c->bp_info.has_value) {
                    // the key is known.  allocate a new item.
                    item* it;
                    size_t value_len;

                    // commands with values must be done over tcp
                    assert(c->udp == 0);
                    
                    // make sure it this is a request that expects a value field.
                    assert(c->u.empty_req.cmd == BP_SET_CMD ||
                           c->u.empty_req.cmd == BP_SETQ_CMD ||
                           c->u.empty_req.cmd == BP_ADD_CMD ||
                           c->u.empty_req.cmd == BP_ADDQ_CMD ||
                           c->u.empty_req.cmd == BP_REPLACE_CMD ||
                           c->u.empty_req.cmd == BP_REPLACEQ_CMD ||
                           c->u.empty_req.cmd == BP_APPEND_CMD ||
                           c->u.empty_req.cmd == BP_APPENDQ_CMD);

                    value_len = ntohl(c->u.key_value_req.body_length) - (sizeof(key_value_req_t) - BINARY_PROTOCOL_REQUEST_HEADER_SZ);
                    value_len -= c->u.key_value_req.keylen;

                    if (settings.detail_enabled) {
                        stats_prefix_record_set(c->bp_key);
                    }
                    
                    if (settings.verbose > 1) {
                        fprintf(stderr, ">%d receiving key %s\n", c->sfd, c->bp_key);
                    }
                    it = item_alloc(c->bp_key, c->u.key_value_req.keylen,
                                    ntohl(c->u.key_value_req.flags), 
                                    realtime(ntohl(c->u.key_value_req.exptime)), 
                                    value_len + 2);
                
                    if (it == NULL) {
                        // this is an error condition.  head straight to the
                        // process state, which must handle this and set the
                        // result field to mc_res_remote_error.
                        c->item = NULL;
                        c->state = conn_bp_process;
                        break;
                    }
                    c->item = it;
                    c->riov_left = item_setup_receive(it, c->riov, false /* do NOT
                                                                            expect
                                                                            CR-LF */);
                    c->riov_curr = 0;

                    // to work with the existing ascii protocol, we have to end
                    // our value string with \r\n.  this is why we allocate
                    // value_len + 2 at item_alloc(..).
                    item_memcpy_to(it, value_len, "\r\n", 2);
                    c->state = conn_bp_waiting_for_value;
                } else {
                    // head to processing.
                    c->state = conn_bp_process;
                }
                
                break;

            case conn_bp_waiting_for_value:
                // we have the key and the value.  proceed straight to
                // processing.
                c->state = conn_bp_process;
                break;

            case conn_bp_waiting_for_string:
                // we have the string.  proceed straight to processing.
                c->state = conn_bp_process;
                break;

            default:
                assert(0);
        }
        
        return retval;
    }

    // try a direct read.
    ssize_t res = readv(c->sfd, &c->riov[c->riov_curr], 
                        c->riov_left <= IOV_MAX ? c->riov_left : IOV_MAX);

    if (res > 0) {
        STATS_LOCK();
        stats.bytes_read += res;
        STATS_UNLOCK();

        while (res > 0) {
            struct iovec* current_iov = &c->riov[c->riov_curr];
            int copied_to_current_iov = current_iov->iov_len <= res ? current_iov->iov_len : res;

            res -= copied_to_current_iov;
            current_iov->iov_base += copied_to_current_iov;
            current_iov->iov_len -= copied_to_current_iov;

            /* are we done with the current IOV? */
            if (current_iov->iov_len == 0) {
                c->riov_curr ++;
                c->riov_left --;
            }
        }
        return retval;
    } 

    if (res == 0) {
        c->state = conn_closing;
    } else if (res == -1 && (errno = EAGAIN || errno == EWOULDBLOCK)) {
        if (!update_event(c, EV_READ | EV_PERSIST)) {
            if (settings.verbose > 0) {
                fprintf(stderr, "Couldn't update event\n");
            }
            c->state = conn_closing;
            return retval;
        }
        retval.stop = 1;
    } else {
        if (settings.verbose > 0) {
            fprintf(stderr, "Failed to read, and not due to blocking\n");
        }
        c->state = conn_closing;
    }
    return retval;
}


static inline bp_handler_res_t handle_process(conn* c)
{
    bp_handler_res_t retval = {0, 0};

    // if we haven't set up the msghdrs structure to hold the outbound messages,
    // do so now.
    if (c->msgused == 0) {
        add_msghdr(c);
    }

    switch (c->u.empty_req.cmd) {
        // these commands go as an empty_req and return as an empty_rep.
        case BP_ECHO_CMD:
            handle_echo_cmd(c);
            break;

        case BP_QUIT_CMD:
            c->state = conn_closing;
            break;
                        
        // these commands go as an empty_req and return as a string_rep.
        case BP_VER_CMD:
            handle_version_cmd(c);
            break;
                        
        // these commands go as a key_req and return as a value_rep.
        case BP_GET_CMD:
        case BP_GETQ_CMD:
            handle_get_cmd(c);
            break;
                        
        // these commands go as a key_value_req and return as an empty_rep.
        case BP_SET_CMD:
        case BP_ADD_CMD:
        case BP_REPLACE_CMD:
        case BP_APPEND_CMD:
                        
        case BP_SETQ_CMD:
        case BP_ADDQ_CMD:
        case BP_REPLACEQ_CMD:
        case BP_APPENDQ_CMD:
            handle_update_cmd(c);
            break;

        // these commands go as a key_number_req and return as an empty_rep.
        case BP_DELETE_CMD:
        case BP_DELETEQ_CMD:
            handle_delete_cmd(c);
            break;
                        
        // these commands go as a key_number_req and return as a number_rep.
        case BP_INCR_CMD:
        case BP_DECR_CMD:
            handle_arith_cmd(c);
            break;
                        
        // these commands go as a number_req and return as an empty_rep.
        case BP_FLUSH_ALL_CMD:
                        
        // these commands go as a string_req and return as an empty_rep.
        case BP_FLUSH_REGEX_CMD:
            assert(0);
                        
        // these commands go as a string_req and return as a string_rep.
        case BP_STATS_CMD:
            assert(0);

        default:
            assert(0);
    }

    return retval;
}


static inline bp_handler_res_t handle_writing(conn* c)
{
    bp_handler_res_t retval = {0, 0};

    switch (bp_transmit(c)) {
        case TRANSMIT_COMPLETE:
            c->icurr = c->ilist;
            while (c->ileft > 0) {
                item *it = *(c->icurr);
                assert((it->it_flags & ITEM_SLABBED) == 0);
                item_remove(it);
                c->icurr++;
                c->ileft--;
            }

            // reset state back to reflect no outbound messages.
            c->state = conn_bp_header_size_unknown;
            c->msgcurr = 0;
            c->msgused = 0;
            c->iovused = 0;
            
            release_reply_headers(c);
            break;

        case TRANSMIT_INCOMPLETE:
        case TRANSMIT_HARD_ERROR:
            break;

        case TRANSMIT_SOFT_ERROR:
            retval.stop = 1;
            break;
    }

    return retval;
}


static void handle_echo_cmd(conn* c)
{
    empty_rep_t* rep;

    if ((rep = ALLOCATE_REPLY_HEADER(c, empty_rep_t, &c->u.empty_req)) == NULL) {
        bp_write_err_msg(c, "out of memory");
        return;
    }

    rep->status = mcc_res_ok;
    rep->body_length = htonl(sizeof(*rep) - BINARY_PROTOCOL_REPLY_HEADER_SZ);

    // nothing special for the echo command to do, so just add ourselves to the
    // list of buffers to transmit.
    if (add_iov(c, rep, sizeof(empty_rep_t), true)) {
        bp_write_err_msg(c, "couldn't build response");
        return;
    }

    if (c->udp && build_udp_headers(c)) {
        bp_write_err_msg(c, "out of memory");
        return;
    }
    c->state = conn_bp_writing;
}


static void handle_version_cmd(conn* c)
{
    string_rep_t* rep;

    if ((rep = ALLOCATE_REPLY_HEADER(c, string_rep_t, &c->u.empty_req)) == NULL) {
        bp_write_err_msg(c, "out of memory");
        return;
    }

    rep->status = mcc_res_ok;
    rep->body_length = htonl(sizeof(VERSION) - 1 + sizeof(*rep) - BINARY_PROTOCOL_REPLY_HEADER_SZ);

    // nothing special for the echo command to do, so just add ourselves to the
    // list of buffers to transmit.
    if (add_iov(c, rep, sizeof(string_rep_t), true) ||
        add_iov(c, VERSION, sizeof(VERSION) - 1, false)) {
        bp_write_err_msg(c, "couldn't build response");
        return;
    }

    if (c->udp && build_udp_headers(c)) {
        bp_write_err_msg(c, "out of memory");
        return;
    }
    c->state = conn_bp_writing;
}


static void handle_get_cmd(conn* c)
{
    value_rep_t* rep;
    item* it;

    // find the desired item.
    it = item_get(c->bp_key, ntohl(c->u.key_req.body_length) - 
                  (sizeof(key_req_t) - BINARY_PROTOCOL_REQUEST_HEADER_SZ));

    // handle the counters.  do this all together because lock/unlock is costly.
    STATS_LOCK();
    stats.get_cmds ++;
    if (it) {
        stats.get_hits ++;
    } else {
        stats.get_misses ++;
    }
    STATS_UNLOCK();
    if (settings.detail_enabled) {
        stats_prefix_record_get(c->bp_key, NULL != it);
    }

    // we only need to reply if we have a hit or if it is a non-silent get.
    if (it ||
        c->u.key_req.cmd == BP_GET_CMD) {
        if ((rep = ALLOCATE_REPLY_HEADER(c, value_rep_t, &c->u.key_req)) == NULL) {
            bp_write_err_msg(c, "out of memory");
            return;
        }
    } else {
        // cmd must have been a getq.
        c->state = conn_bp_header_size_unknown;
        return;
    }

    if (it) {
        // the cache hit case.
        if (c->ileft >= c->isize) {
            item **new_list = realloc(c->ilist, sizeof(item *)*c->isize*2);
            if (new_list) {
                c->isize *= 2;
                c->ilist = new_list;
            } else {
                bp_write_err_msg(c, "out of memory");
                return;
            }
        }
        *(c->ilist + c->ileft) = it;
        item_update(it);
        
        STATS_LOCK();
        stats.get_hits++;
        stats_size_buckets_get(it->nkey + it->nbytes);
        STATS_UNLOCK();

        // fill out the headers.
        rep->status = mcc_res_found;
        rep->body_length = htonl((sizeof(*rep) - BINARY_PROTOCOL_REPLY_HEADER_SZ) +
                                 it->nbytes - 2); // chop off the '\r\n'

        if (add_iov(c, rep, sizeof(value_rep_t), true) ||
            add_item_to_iov(c, it, false /* don't send cr-lf */)) {
            bp_write_err_msg(c, "couldn't build response");
            return;
        }

        if (settings.verbose > 1) {
            fprintf(stderr, ">%d sending key %s\n", c->sfd, ITEM_key(it));
        }
    } else {
        STATS_LOCK();
        stats.get_misses++;
        STATS_UNLOCK();

        if (c->u.key_req.cmd == BP_GET_CMD) {
            // cache miss on the terminating GET command.
            rep->status = mcc_res_notfound;
            rep->body_length = htonl((sizeof(*rep) - BINARY_PROTOCOL_REPLY_HEADER_SZ));
            
            if (add_iov(c, rep, sizeof(value_rep_t), true)) {
                bp_write_err_msg(c, "couldn't build response");
                return;
            }
        }
    }

    // if it is a quiet request, then wait for the next request
    if (c->u.key_req.cmd == BP_GETQ_CMD) {
        c->state = conn_bp_header_size_unknown;
    } else { 
        c->state = conn_bp_writing;

        if (c->udp && build_udp_headers(c)) {
            bp_write_err_msg(c, "out of memory");
            return;
        }
    }
}


static void handle_update_cmd(conn* c)
{
    empty_rep_t* rep;
    item* it = c->item;
    int comm, quiet = 1;

    if ((rep = ALLOCATE_REPLY_HEADER(c, empty_rep_t, &c->u.key_value_req)) == NULL) {
        bp_write_err_msg(c, "out of memory");
        return;
    }

    STATS_LOCK();
    stats.set_cmds ++;
    STATS_UNLOCK();

    switch (c->u.key_value_req.cmd) {
        case BP_SET_CMD:
            quiet = 0;
        case BP_SETQ_CMD:
            comm = NREAD_SET;
            break;

        case BP_ADD_CMD:
            quiet = 0;
        case BP_ADDQ_CMD:
            comm = NREAD_ADD;
            break;

        case BP_REPLACE_CMD:
            quiet = 0;
        case BP_REPLACEQ_CMD:
            comm = NREAD_REPLACE;
            break;

        default:
            assert(0);
            bp_write_err_msg(c, "Can't be here.\n");
            return;
    }

    if (settings.verbose > 1) {
        fprintf(stderr, ">%d received key %s\n", c->sfd, c->bp_key);
    }
    if (store_item(it, comm)) {
        rep->status = mcc_res_stored;
    } else {
        rep->status = mcc_res_notstored;
    }
    rep->body_length = htonl(sizeof(*rep) - BINARY_PROTOCOL_REPLY_HEADER_SZ);

    item_remove(c->item);
    c->item = NULL;

    if (add_iov(c, rep, sizeof(empty_rep_t), true)) {
        bp_write_err_msg(c, "couldn't build response");
        return;
    }

    // if it is a quiet request, then wait for the next request
    if (quiet) {
        c->state = conn_bp_header_size_unknown;
    } else { 
        c->state = conn_bp_writing;
    }
}


static void handle_delete_cmd(conn* c)
{
    empty_rep_t* rep;
    item* it;
    size_t key_length = c->u.key_number_req.keylen;
    time_t exptime = ntohl(c->u.key_number_req.number);

    if (settings.detail_enabled) {
        stats_prefix_record_delete(c->bp_key);
    }

    it = item_get(c->bp_key, key_length);

    if (it ||
        c->u.key_number_req.cmd == BP_DELETE_CMD) {
        if ((rep = ALLOCATE_REPLY_HEADER(c, empty_rep_t, &c->u.key_number_req)) == NULL) {
            bp_write_err_msg(c, "out of memory");
            return;
        }

        rep->body_length = htonl(sizeof(*rep) - BINARY_PROTOCOL_REPLY_HEADER_SZ);
    } else {
        // cmd must have been a getq.
        c->state = conn_bp_header_size_unknown;
        return;
    }

    if (it) {
        if (exptime == 0) {
            STATS_LOCK();
            stats_size_buckets_delete(it->nkey + it->nbytes);
            STATS_UNLOCK();
            
            item_unlink(it, UNLINK_NORMAL);
            item_remove(it);            // release our reference
            rep->status = mcc_res_deleted;
        } else {
            switch (defer_delete(it, exptime)) {
                case 0:
                    rep->status = mcc_res_deleted;
                    break;
                    
                case -1:
                    bp_write_err_msg(c, "out of memory");
                    return;

                default:
                    assert(0);
            }
        }
    } else {
        rep->status = mcc_res_notfound;
    }

    if (add_iov(c, rep, sizeof(empty_rep_t), true)) {
        bp_write_err_msg(c, "couldn't build response");
    }

    // if it is a quiet request, then wait for the next request
    if (c->u.key_number_req.cmd == BP_DELETEQ_CMD) {
        c->state = conn_bp_header_size_unknown;
    } else { 
        c->state = conn_bp_writing;

        if (c->udp && build_udp_headers(c)) {
            bp_write_err_msg(c, "out of memory");
            return;
        }
    }
}


static void handle_arith_cmd(conn* c)
{
    number_rep_t* rep;
    item* it;
    size_t key_length = c->u.key_number_req.keylen;
    uint32_t delta;
    static char temp[32];

    it = item_get(c->bp_key, key_length);

    if ((rep = ALLOCATE_REPLY_HEADER(c, number_rep_t, &c->u.key_number_req)) == NULL) {
        bp_write_err_msg(c, "out of memory");
        return;
    }
    rep->body_length = htonl(sizeof(*rep) - BINARY_PROTOCOL_REPLY_HEADER_SZ);
    
    if (it) {
        char* out;
        uint32_t val;
        delta = ntohl(c->u.key_number_req.number);
        out = add_delta(it, (c->u.key_number_req.cmd == BP_INCR_CMD),
                        delta, temp, &val);

        if (out != temp) {
            // some error occured.
            if (strncmp("CLIENT_ERROR", out, sizeof("CLIENT_ERROR") - 1) == 0) {
                rep->status = mcc_res_local_error;
            } else {
                rep->status = mcc_res_remote_error;
            }
        } else {
            // stored.
            rep->value = val;
            rep->status = mcc_res_stored;
        }
    } else {
        rep->status = mcc_res_notfound;
    }

    if (add_iov(c, rep, sizeof(number_rep_t), true)) {
        bp_write_err_msg(c, "couldn't build response");
    }

    c->state = conn_bp_writing;
    
    if (c->udp && build_udp_headers(c)) {
        bp_write_err_msg(c, "out of memory");
        return;
    }
}


static void* allocate_reply_header(conn* c, size_t size, void* req)
{
    empty_req_t* srcreq = (empty_req_t*) req;
    empty_rep_t* retval;

    // do we have enough space?
    if (c->bp_hdr_pool->bytes_free < size) {
        if ((c->bp_hdr_pool = bp_allocate_hdr_pool(c->bp_hdr_pool)) == NULL) {
            return NULL;
        }
    }

    retval = (empty_rep_t*) c->bp_hdr_pool->ptr;
    c->bp_hdr_pool->ptr += size;
    c->bp_hdr_pool->bytes_free -= size;
    retval->magic = BP_REP_MAGIC_BYTE;
    retval->cmd = srcreq->cmd;
    retval->reserved = 0;
    retval->opaque = srcreq->opaque;

    return retval;
}


static void release_reply_headers(conn* c)
{
    bp_hdr_pool_t* bph;
    while (c->bp_hdr_pool->next != NULL) {
        bph = c->bp_hdr_pool;
        c->bp_hdr_pool = c->bp_hdr_pool->next;
        free(bph);
    }
}


static void bp_write_err_msg(conn* c, const char* str)
{
    string_rep_t* rep;

    rep = (string_rep_t*) c->wbuf;
    rep->magic = BP_REP_MAGIC_BYTE;
    rep->cmd = BP_SERVERERR_CMD;
    rep->status = mcc_res_remote_error;
    rep->reserved = 0;
    rep->opaque = 0;
    rep->body_length = htonl(strlen(str) + (sizeof(*rep) - BINARY_PROTOCOL_REPLY_HEADER_SZ));
    
    if (add_iov(c, c->wbuf, sizeof(string_rep_t), true) ||
        (c->udp && build_udp_headers(c))) {
        if (settings.verbose > 0) {
            fprintf(stderr, "Couldn't build response\n");
        }
        c->state = conn_closing;
        return;
    }

    c->state = conn_bp_error;
}


/*
 * read a UDP request.
 * return 0 if there's nothing to read.
 */
static int bp_try_read_udp(conn *c) {
    int res;

    c->request_addr_size = sizeof(c->request_addr);
    res = recvfrom(c->sfd, c->rbuf, c->rsize,
                   0, &c->request_addr, &c->request_addr_size);
    if (res > 8) {
        unsigned char *buf = (unsigned char *)c->rbuf;
        STATS_LOCK();
        stats.bytes_read += res;
        STATS_UNLOCK();

        /* Beginning of UDP packet is the request ID; save it. */
        c->request_id = buf[0] * 256 + buf[1];

        /* If this is a multi-packet request, drop it. */
        if (buf[4] != 0 || buf[5] != 1) {
            bp_write_err_msg(c, "multi-packet request not supported");
            return 0;
        }

        /* Don't care about any of the rest of the header. */
        res -= 8;
        memmove(c->rbuf, c->rbuf + 8, res);

        c->rbytes += res;
        c->rcurr = c->rbuf;
        return 1;
    }
    return 0;
}

/*
 * read from network as much as we can, handle buffer overflow and connection
 * close.
 * before reading, move the remaining incomplete fragment of a command
 * (if any) to the beginning of the buffer.
 * return 0 if there's nothing to read on the first read.
 */
static int bp_try_read_network(conn *c) {
    int gotdata = 0;
    int res;

    if (c->rcurr != c->rbuf) {
        if (c->rbytes != 0) /* otherwise there's nothing to copy */
            memmove(c->rbuf, c->rcurr, c->rbytes);
        c->rcurr = c->rbuf;
    }

    while (1) {
        if (c->rbytes >= c->rsize) {
            char *new_rbuf = realloc(c->rbuf, c->rsize*2);
            if (!new_rbuf) {
                if (settings.verbose > 0) {
                    fprintf(stderr, "Couldn't realloc input buffer\n");
                }
                bp_write_err_msg(c, "out of memory");
                return 1;
            }
            c->rcurr  = c->rbuf = new_rbuf;
            c->rsize *= 2;
        }

        /* unix socket mode doesn't need this, so zeroed out.  but why
         * is this done for every command?  presumably for UDP
         * mode.  */
        if (!settings.socketpath) {
            c->request_addr_size = sizeof(c->request_addr);
        } else {
            c->request_addr_size = 0;
        }

        res = read(c->sfd, c->rbuf + c->rbytes, c->rsize - c->rbytes);
        if (res > 0) {
            STATS_LOCK();
            stats.bytes_read += res;
            STATS_UNLOCK();
            gotdata = 1;
            c->rbytes += res;
            continue;
        }
        if (res == 0) {
            /* connection closed */
            c->state = conn_closing;
            return 1;
        }
        if (res == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            else return 0;
        }
    }
    return gotdata;
}


/*
 * Transmit the next chunk of data from our list of msgbuf structures.
 *
 * Returns:
 *   TRANSMIT_COMPLETE   All done writing.
 *   TRANSMIT_INCOMPLETE More data remaining to write.
 *   TRANSMIT_SOFT_ERROR Can't write any more right now.
 *   TRANSMIT_HARD_ERROR Can't write (c->state is set to conn_closing)
 */
static int bp_transmit(conn *c) {
    int res;

    if (c->msgcurr < c->msgused &&
            c->msglist[c->msgcurr].msg_iovlen == 0) {
        /* Finished writing the current msg; advance to the next. */
        c->msgcurr++;
    }
    if (c->msgcurr < c->msgused) {
        struct msghdr *m = &c->msglist[c->msgcurr];
        res = sendmsg(c->sfd, m, 0);
        if (res > 0) {
            STATS_LOCK();
            stats.bytes_written += res;
            STATS_UNLOCK();

            /* We've written some of the data. Remove the completed
               iovec entries from the list of pending writes. */
            while (m->msg_iovlen > 0 && res >= m->msg_iov->iov_len) {
                res -= m->msg_iov->iov_len;
                m->msg_iovlen--;
                m->msg_iov++;
            }

            /* Might have written just part of the last iovec entry;
               adjust it so the next write will do the rest. */
            if (res > 0) {
                m->msg_iov->iov_base += res;
                m->msg_iov->iov_len -= res;
            }
            return TRANSMIT_INCOMPLETE;
        }
        if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (!update_event(c, EV_WRITE | EV_PERSIST)) {
                if (settings.verbose > 0) {
                    fprintf(stderr, "Couldn't update event\n");
                }
                c->state = conn_closing;
                return TRANSMIT_HARD_ERROR;
            }
            return TRANSMIT_SOFT_ERROR;
        }
        /* if res==0 or res==-1 and error is not EAGAIN or EWOULDBLOCK,
           we have a real error, on which we close the connection */
        if (settings.verbose > 0) {
            perror("Failed to write, and not due to blocking");
        }

        if (c->udp) {
            c->state = conn_bp_header_size_unknown;
        } else {
            c->state = conn_closing;
        }
        return TRANSMIT_HARD_ERROR;
    } else {
        return TRANSMIT_COMPLETE;
    }
}

