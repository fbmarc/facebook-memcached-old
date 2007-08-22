/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#if !defined(_facebook_memcache_binary_protocol_h_)
#define _facebook_memcache_binary_protocol_h_

#include <stdint.h>

// version number and magic byte.
#define BP_VERSION         0x0
#define BP_REQ_MAGIC_BYTE  (0x50 | BP_VERSION)
#define BP_REP_MAGIC_BYTE  (0xA0 | BP_VERSION)

#define BIT(x)             (1ULL << (x))
#define FIELD(val, shift)  ((val) << (shift))

#define BP_E_E             FIELD(0x0, 4)
#define BP_E_S             FIELD(0x1, 4)
#define BP_K_V             FIELD(0x2, 4)
#define BP_KV_E            FIELD(0x3, 4)
#define BP_KN_E            FIELD(0x4, 4)
#define BP_KN_N            FIELD(0x5, 4)
#define BP_N_E             FIELD(0x6, 4)
#define BP_S_E             FIELD(0x7, 4)
#define BP_S_S             FIELD(0x8, 4)

#define BP_QUIET           BIT(3)

typedef enum bp_cmd {
    // these commands go as an empty_req and return as an empty_rep.
    BP_ECHO_CMD        = (BP_E_E | FIELD(0x0, 0)),
    BP_QUIT_CMD        = (BP_E_E | FIELD(0x1, 0)),

    // these commands go as an empty_req and return as a string_rep.
    BP_VER_CMD         = (BP_E_S | FIELD(0x0, 0)),
    BP_SERVERERR_CMD   = (BP_E_S | FIELD(0x1, 0)), // this is actually not a
                                                   // command.  this is solely
                                                   // used as a response when
                                                   // the server wants to
                                                   // indicate an error status.

    // these commands go as a key_req and return as a value_rep.
    BP_GET_CMD         = (BP_K_V | FIELD(0x0, 0)),
    BP_GETQ_CMD        = (BP_K_V | BP_QUIET | FIELD(0x0, 0)),

    // these commands go as a key_value_req and return as an empty_rep.
    BP_SET_CMD         = (BP_KV_E | FIELD(0x0, 0)),
    BP_ADD_CMD         = (BP_KV_E | FIELD(0x1, 0)),
    BP_REPLACE_CMD     = (BP_KV_E | FIELD(0x2, 0)),
    BP_APPEND_CMD      = (BP_KV_E | FIELD(0x3, 0)),

    BP_SETQ_CMD        = (BP_KV_E | BP_QUIET | FIELD(0x0, 0)),
    BP_ADDQ_CMD        = (BP_KV_E | BP_QUIET | FIELD(0x1, 0)),
    BP_REPLACEQ_CMD    = (BP_KV_E | BP_QUIET | FIELD(0x2, 0)),
    BP_APPENDQ_CMD     = (BP_KV_E | BP_QUIET | FIELD(0x3, 0)),

    // these commands go as a key_number_req and return as an empty_rep.
    BP_DELETE_CMD      = (BP_KN_E | FIELD(0x0, 0)),
    BP_DELETEQ_CMD     = (BP_KN_E | BP_QUIET | FIELD(0x0, 0)),

    // these commands go as a key_number_req and return as a number_rep.
    BP_INCR_CMD        = (BP_KN_N | FIELD(0x0, 0)),
    BP_DECR_CMD        = (BP_KN_N | FIELD(0x1, 0)),

    // these commands go as a number_req and return as an empty_rep.
    BP_FLUSH_ALL_CMD   = (BP_N_E | FIELD(0x0, 0)),

    // these commands go as a string_req and return as an empty_rep.
    BP_FLUSH_REGEX_CMD = (BP_S_E | FIELD(0x0, 0)),

    // these commands go as a string_req and return as a string_rep.
    BP_STATS_CMD       = (BP_S_S | FIELD(0x0, 0)),
} bp_cmd_t;

// these commands 

#define BINARY_PROTOCOL_REQUEST_HEADER \
    uint8_t magic; \
    uint8_t cmd; \
    uint8_t keylen; \
    uint8_t reserved; \
    uint32_t opaque; \
    uint32_t body_length; \

#define BINARY_PROTOCOL_REPLY_HEADER \
    uint8_t magic; \
    uint8_t cmd;    \
    uint8_t status; \
    uint8_t reserved; \
    uint32_t opaque; \
    uint32_t body_length; \


#define BINARY_PROTOCOL_REQUEST_HEADER_SZ               \
    sizeof(struct { BINARY_PROTOCOL_REQUEST_HEADER })

#define BINARY_PROTOCOL_REPLY_HEADER_SZ               \
    sizeof(struct { BINARY_PROTOCOL_REPLY_HEADER })


typedef struct empty_req_s {
    // this handles the following requests:
    //  version
    //  echo
    //  quit
    BINARY_PROTOCOL_REQUEST_HEADER;
} empty_req_t;

typedef struct key_req_s {
    // this handles the following requests:
    //  get
    //  getq
    BINARY_PROTOCOL_REQUEST_HEADER;
    // key goes here.
} key_req_t;

typedef struct key_value_req_s {
    // this handles the following requests:
    //  set/add/replace
    //  append?
    BINARY_PROTOCOL_REQUEST_HEADER;
    uint32_t exptime;
    uint32_t flags;
    // key goes here.
    // value goes here.
} key_value_req_t;

typedef struct key_number_req_s {
    // this handles the following requests:
    //  delete
    //  incr/decr
    BINARY_PROTOCOL_REQUEST_HEADER;
    uint32_t number;
    // key goes here.
} key_number_req_t;

typedef struct number_req_s {
    // this handles the following requests:
    //  flush_all
    BINARY_PROTOCOL_REQUEST_HEADER;
    uint32_t number;
} number_req_t;

typedef struct string_req_s {
    // this handles the following requests:
    //  stats <extended>
    //  flush_regex
    BINARY_PROTOCOL_REQUEST_HEADER;
} string_req_t;

typedef struct empty_rep_s {
    // this handles the following replies:
    //  echo
    //  quit
    //  flush_all
    //  flush_regex
    //  delete
    //  set/add/replace
    //  append?
    BINARY_PROTOCOL_REPLY_HEADER;
} empty_rep_t;

typedef struct value_rep_s {
    // this handles the following replies:
    //  get
    //  getr
    BINARY_PROTOCOL_REPLY_HEADER;
    uint32_t exptime;
    uint32_t flags;
    // value goes here.
} value_rep_t;

typedef struct number_rep_s {
  // this handles the following replies:
  //  incr/decr
    BINARY_PROTOCOL_REPLY_HEADER;
    uint32_t value;
} number_rep_t;

typedef struct string_rep_s {
  // this handles the following replies:
  //  stats
  //  ver
    BINARY_PROTOCOL_REPLY_HEADER;
    // string goes here.
} string_rep_t;

#endif /* #if !defined(_facebook_memcache_binary_protocol_h_) */
