/*
 * This is a load generator for memcached.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdbool.h>
#include <poll.h>
#include <signal.h>
#include <netdb.h>
#include <math.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include "sysqueue.h"
#include "cpu.h"


#define MAXSIZE 64000 /* max key value size in bytes */
#define MAXKEYS 10000000 /* max number of keys */

#define MAXRTT  10000 /* max bucketized RTT is 10000us */
#define RTTBUCKET 100 /* 100us per bucket */

#define STACKSIZE (1024*1024) /* thread stack size in bytes */

#define KEYPREFIX "k" /* key format is KEYPREFIX-<num> */

#define RQWHEELCAP 100 /* the number of seconds worth of requests that a
                        request wheel must be able to hold (ex: at 100 req/s
                        must hold 100*RQWHEELCAP requests) */

#define max(a,b) ((a)>(b) ? (a) : (b))
#define min(a,b) ((a)<(b) ? (a) : (b))

#define pre(n, size) ((n>0) ? (n)-1 : (size)-1)
#define succ(n, size) (((n)+1) % (size))

extern double get_cpu_frequency(void);


/* request types */
typedef enum { req_get, req_set, reqtype_n } reqtype_t;

static const char *reqtype_str[] = {"get", "set", NULL};

/* protocol types */
typedef enum { proto_tcp, proto_udp } proto_t;


/* UDP packet header */
typedef struct udphdr_s {
  uint16_t rqid;
  uint16_t partno;
  uint16_t nparts;
  uint16_t reserved;
} udphdr_t;


/* statistics */
typedef struct stats_s {
  uint64_t rtt_total,  /* the sum of all rtts measured so far */
           rtt_min,    /* min and max measured rtt in ticks */
           rtt_max;

  uint64_t rtt_buckets[MAXRTT/RTTBUCKET+1]; /* request counts bucketized into
                                               time intervals */
  uint64_t nmeasured, /* the number of RTT samples measured */
           nslow,     /* number of requests that were measured to take
                        over MAXRTT microseconds */
           ntimedout, /* number of requests that were not measured */
           nsent,     /* number of sent requests */
           nfailed,   /* number of reported errors */
           nbogus;    /* number of malformed replies */
} stats_t;



/* request */
typedef struct req_s {
  uint32_t id;          /* 32-bit id, only used for UDP gets. The low 16 bits
                           are sent and echoed. */
  int npartsleft;       /* for UDP replies: the number of reply parts
                           left to receive, -1 for TCP requests and for UDP
                           requests that have not yet seen any reply parts */
  reqtype_t type;       /* set, get */
  int key;              /* key requested */
  uint64_t tsent;       /* clock tick at which this request was sent, 0 if
                           not yet sent */
  uint64_t treply;      /* clock tick at which a reply was received, 0 if
                           no reply yet */
} req_t;


/* request wheel -- a circular buffer holding a queue of requests */
typedef struct rqwheel_s {
  int tail;             /* offset of oldest request in queue */
  int head;             /* offset of the slot following latest request in
                           queue. head==tail when queue is empty */
  req_t *rqs;           /* array of requests */
  int size;             /* number of slots in .rqs */
  uint32_t nextrqid;    /* id to assign to next request */
  struct thread_s *th;  /* backpointer to thread */
} rqwheel_t;


/* a non-blocking buffered TCP connection */
typedef struct conn_s {
  int s;

  rqwheel_t reqs; /* outstanding requests. Next complete reply received
                     on this conn matches the oldest one of these. */

  int sndkey;    /* the id of key currently being set/requested through
                    this conn */
  char *sndbuf;  /* a linear (not circular) buffer where messages are
                    composed before sending */
  int sndbufsize; /* the size of .sndbuf array in bytes */
  int sndoff;    /* offset of next byte in sndbuf to be sent */
  int sndlen;    /* total size of message in .sndbuf */

  char *rcvbuf;  /* a circular buffer where replies are assembled before
                    they are parsed. It should be at least
                    max(so_rcvbuf, maxmsgsize) bytes in size. */
  int rcvbufsize;/* the size of .rcvbuf array in bytes */
  int rcvstart;  /* offset of the first byte of message being parsed */
  int rcvnext;   /* offset that next byte will be read into. The buffered
                    (partial) message is between rcvstart and rcvnext modulo
                    rcvbufsize */
  int rkey;      /* if current reply is a VALUE, this is the key that came with
                    the reply, otherwise -1 */
  reqtype_t rtype;   /* reply type, if known, otherwise -1 */
  int nskipsep; /* the expected number of \r\n separators before next
                      message, or 0 if that number is unknown */

  bool ineventq; /* true if this conn is in its thread .eventq */
  TAILQ_ENTRY(conn_s) link; /* links connections into a queue of connections
                               that may have events on them */
} conn_t;


/* A datagram access point. This is the counterpart of conn_t for
   datagram communication. */
typedef struct dgram_ap_s {
  int s;            /* UDP socket for sending requests out */
  rqwheel_t reqs;   /* outstanding requests sent into .s */
  char *rcvbuf;     /* buffer into which datagrams are read */
  int rcvbufsize;   /* size of .rcvbuf in bytes */
} dgram_ap_t;


typedef struct thread_s {
  pthread_t pt;        /* pthread handle */
  int cpu;             /* cpu this thread is bound to: 0, 1, 2 ... */
  conn_t *conns;       /* an array of TCP connections */
  dgram_ap_t udp;      /* UDP access point */
  uint64_t tstart;     /* main loop start time in ticks */
  uint64_t tend;       /* thread end time in ticks */
  stats_t stats[reqtype_n]; /* statistics by request type */
  double  cpufreq;     /* cpu frequency in ticks per usec (XXX double?) */

  /*DEBUG*/uint64_t nufds; /* # ufd structures passed to all poll() calls */
  /*DEBUG*/uint64_t npolls; /* # poll calls */

  TAILQ_HEAD(eventq_s, conn_s) eventq; /* connections that should be
                                              polled for events */


  volatile bool done;  /* terminate main loop. This is set by main thread
                          on SIGINT and SIGALRM */
} thread_t;


static const char *hostname;     /* host where memcached runs */
static struct in_addr hostaddr;  /* ip address of host */
static char hostaddr_str[32];    /* ip address of host as a string */
static int port_tcp;
static int port_udp;
static int nthreads;    /* number of threads to run */
static int nconns=1;    /* number of connections each thread opens */
static int nkeys=1;
static int nreplyports;
static int rates[reqtype_n]; /* try to send this many
                                sets and gets per second */
static int valsz=100;
static int duration;    /* run duration in seconds */
static bool quiet=false;
static bool nodelay=true; /* set TCP nodelay */
static int socksndbufsz; /* socket sendbuf size, 0 to use system defaults */

static struct sockaddr_in hostaddr_udp; /* ip:port for UDP requests */
static struct sockaddr_in hostaddr_tcp; /* ip:port for TCP requests */

static thread_t *threads; /* thread-specific data for all worker threads */


static void die(const char *fmt, ...) {
  va_list ap;

  va_start(ap, fmt);

  vfprintf(stderr, fmt, ap);
  fprintf(stderr, "\n");

  va_end(ap);

  exit(1);
}


void stopthreads(int sig) {
  int i;

  for (i=0; i<nthreads; i++)
    threads[i].done = true;

}


static void stats_init(stats_t *st) {
  memset(st, 0, sizeof(*st));
  st->rtt_min = (uint64_t)-1;
}


static inline void
stats_update_rtts(stats_t *st, uint64_t tsent, uint64_t treply, double cpufreq) {
  uint64_t rtt = treply - tsent;

  if (rtt < st->rtt_min)
    st->rtt_min = rtt;
  if (rtt > st->rtt_max)
    st->rtt_max = rtt;

  st->rtt_total += rtt;
  st->nmeasured++;

  if (rtt / cpufreq >= MAXRTT)
    st->nslow++;
  else
    st->rtt_buckets[(unsigned)(rtt / cpufreq / RTTBUCKET)]++;
}


static void rqwheel_init(rqwheel_t *w, int size, thread_t *th) {
  memset(w, 0, sizeof(*w));
  w->size = size;
  w->nextrqid = 0;
  w->th = th;
  w->rqs = (req_t*)calloc(size, sizeof(req_t));
  if (!w->rqs) {
    die("calloc() failed in rqwheel_init()");
  }
}


static inline bool rqwheel_isempty(rqwheel_t *w) {
  return w->head == w->tail;
}

static inline void req_init(req_t *rq, reqtype_t t, uint32_t id, int k) {
  rq->id = id;
  rq->npartsleft = -1;
  rq->type = t;
  rq->key = k;
  rq->tsent = cycle_timer();
  rq->treply = 0;
}


/* Make a note of a new outstanding request that we just sent out. */
static inline void rqwheel_append_request(rqwheel_t *w, reqtype_t t, int k) {

  /*DEBUG
  if (w->th->stats[t].nsent % 100 == 0) {
    printf("Appending '%s' request with id %u, nsent=%lu w->tail=%d "
           "w->head=%d\n", reqtype_str[t], w->nextrqid, w->th->stats[t].nsent,
           w->tail, w->head);
  }*/

  req_init(&w->rqs[w->head], t, w->nextrqid++, k);
  w->head = succ(w->head, w->size);
  w->th->stats[t].nsent++;

  if (w->head == w->tail) {
    /* the queue is full, count message at .head as timed out. We do it here
       rather than during reply handling because replies may be lost. */
    w->th->stats[w->rqs[w->tail].type].ntimedout++;
    do {
      w->tail = succ(w->tail, w->size);
    } while (w->tail != w->head && w->rqs[w->tail].treply > 0);
  }

}


static inline void rqwheel_update_tsent(rqwheel_t *w) {
  int last = (w->head == 0 ? w->size-1 : w->head-1);

  w->rqs[last].tsent = cycle_timer();
}


/* Verify that the parameters of reply (if non-negative) match the oldest
   outstanding request in _w_ and update the stats. */
static inline void
rqwheel_note_tcp_reply(rqwheel_t *w, reqtype_t t, int k) {
  req_t *rq = &w->rqs[w->tail]; /* expected matching request */

  if (rqwheel_isempty(w)) {
    /* Ignore this reply. */
    if (!quiet) {
      fprintf(stderr, "Got a TCP reply for a %s for key %s with empty "
              "request queue!\n", reqtype_str[t], k);
    }
  }

  if (t >= 0 && rq->type != t) {
    if (!quiet) {
      fprintf(stderr, "Got a TCP reply of type %s, expected %s\n",
              reqtype_str[t], reqtype_str[rq->type]);
    }
    w->th->stats[t].nbogus++;
  }

  if (k >= 0 && rq->key != k) {
    if (!quiet) {
      fprintf(stderr, "Got a TCP reply of type %s for key %d, "
              "expected key %d\n", reqtype_str[t], k, rq->key);
    }
    w->th->stats[t].nbogus++;
  }

  /* reply is good, mark request completed */

  rq->treply = cycle_timer();

  stats_update_rtts(&w->th->stats[t], rq->tsent, rq->treply, w->th->cpufreq);

  w->tail = succ(w->tail, w->size);
}


/* Match up a datagram reply with an outstanding request and update the
   stats. _rs_ is the UDP header received in reply. _k_ is the key in VALUE
   reply or -1 if the reply indicated that the key was not found or an
   error occurred. */
static inline void
rqwheel_note_udp_reply(rqwheel_t *w, udphdr_t rs, int k) {
  int match; /* expected position of the corresponding request in w->rqs[] */
  int last = (w->head == 0 ? w->size-1 : w->head-1);
  uint16_t rqdistance = rs.rqid - (uint16_t)w->rqs[w->tail].id;
  req_t *rq; /* matching request */

  /* try to locate the request record for rs.rqid */

  if (rqwheel_isempty(w)) {
    /* request wheel is empty. Ignore this reply. */
    if (!quiet) {
      fprintf(stderr, "Got a UDP reply with id %d for key %s with empty "
              "request queue!\n", (int)rs.rqid, k);
    }
  }

  match = (w->tail + (uint32_t)rqdistance) % w->size;

  /* verify that _match_ is in [tail..last] modulo w->size. If it is not,
     the reply is for a request that's no longer in the queue. The timedout
     counter has already been incremented in rqwheel_append_request(). */
  if (w->tail <= last) {
    if (match < w->tail || last < match) {
      return;
    }
  }
  else if (last < match && match < w->tail) {
      return;
  }

  rq = &w->rqs[match];

    /* request ids wrapped around AND the matching request is no longer in
       the queue. The request has already been counted as timed out in
       rqwheel_append_request() that bumped the request record off
       the queue. */
  if ((uint16_t)rq->id != rs.rqid) {
    if (!quiet) {
      fprintf(stderr, "Got reply for request id %u, expected %u\n",
              (unsigned)rs.rqid, (unsigned)(rq->id & 0xffff));
    }
    return;
  }
  if (rq->key != k && k >= 0) {
    if (!quiet) {
      fprintf(stderr, "Got reply for a 'get' with key %d, expected key %d\n",
              k, rq->key);
    }
    return;
  }

  if (rq->npartsleft < 0) {
    /* got first reply part for this request */
    rq->npartsleft = (unsigned)rs.nparts;
  }
  else if (rq->npartsleft == 0) {
    if (!quiet) {
      fprintf(stderr, "Got a duplicate reply for 'get' request %u "
              "for key %d\n", rq->id, rq->key);
      return;
    }
  }

  rq->npartsleft--;

  if (rs.partno == 0 && k < 0 && !quiet) {
    fprintf(stderr, "'get' request for key %d failed\n", rq->key);
  }

  if (rq->npartsleft > 0)
    return;

  /* we got all reply parts, mark request completed */

  rq->treply = cycle_timer();

  stats_update_rtts(&w->th->stats[req_get], rq->tsent, rq->treply,
                    w->th->cpufreq);

  if (match == w->tail) {
    /* trim the request queue by moving tail forward to the oldest request
       that is still outstanding */
    while (w->tail != w->head && w->rqs[w->tail].treply)
      w->tail = succ(w->tail, w->size);
  }
}


static int getbufsize(int sock, int which) {
  int bufsize;
  socklen_t optsize = sizeof(bufsize);
  int rv;

  rv = getsockopt(sock, SOL_SOCKET, which, &bufsize, &optsize);
  if (rv != 0)
    die("getsockopt failed");

  return bufsize;
}


static int setbufsize(int sock, int which, int bufsize) {
  int rv;

  rv = setsockopt(sock, SOL_SOCKET, which, &bufsize, sizeof(bufsize));
  if (rv != 0)
    die("setsockopt() failed: %s\n", strerror(errno));

  return getbufsize(sock, which);
}


/* Initialize a new conn object, establish a connection to <hostname>:<port>,
   maxoutstanding is the size of request wheel to allocate, maxmsgsize is
   the maximum allowed message size in bytes */
static void conn_init(conn_t *conn, int maxoutstanding, int maxmsgsize,
                      thread_t *th) {
  int rv;
  int rcvbufsize;

  memset(conn, 0, sizeof(*conn));

  conn->s = newsocket();
  if (conn->s < 0) {
    die("socket() failed: %s\n", strerror(errno));
  }

  rv = connect(conn->s, (struct sockaddr*)&hostaddr_tcp, sizeof(hostaddr_tcp));
  if (rv != 0) {
    die("Failed to connect to %s:%d : %s\n", hostaddr_str, port_tcp,
        strerror(errno));
  }

  set_nonblock(conn->s);
  set_nodelay(conn->s, (nodelay ? 1 : 0));

  rqwheel_init(&conn->reqs, maxoutstanding, th);

  conn->sndbufsize = maxmsgsize+1024;
  conn->sndbuf = malloc(conn->sndbufsize);
  if (!conn->sndbuf)
    die("malloc(sndbuf) failed");

  if (socksndbufsz > 0) {
    setbufsize(conn->s, SO_SNDBUF, socksndbufsz);
  }

  rcvbufsize = getbufsize(conn->s, SO_RCVBUF) + 1024;

  conn->rcvbuf = malloc(max(maxmsgsize, rcvbufsize)+1024);

  if (!conn->rcvbuf)
    die("malloc(rcvbuf) failed");

  conn->rcvbufsize = rcvbufsize;

  conn->ineventq = false;
}


static inline int compose_get(char *buf, int bufsize, int k) {
  return snprintf(buf, bufsize, "get " KEYPREFIX "-%d\r\n", k);
}

static inline int compose_set(char *buf, int bufsize, int k) {
  int rqhdrlen = snprintf(buf, bufsize, "set " KEYPREFIX
                                        "-%d 0 0 %d\r\n", k, valsz);

  if (rqhdrlen >= bufsize-1 || rqhdrlen+valsz+3 > bufsize) {
    die("Internal error: bufsize (%d) is too small in compose_set(). "
        "Need %d bytes\n", bufsize, rqhdrlen+valsz+3);
  }

  memset(buf+rqhdrlen, ' ', min(8, bufsize-rqhdrlen));
  strcpy(buf+rqhdrlen+valsz, "\r\n");

  return rqhdrlen+valsz+2;
}


static inline bool conn_has_events(conn_t * conn) {
  return conn->sndoff < conn->sndlen || !rqwheel_isempty(&conn->reqs);
}


static inline int conn_events(conn_t *conn, struct pollfd *ufd) {

  if (!conn_has_events(conn)) {
    return 0;
  }

  ufd->fd = conn->s;
  ufd->events = ufd->revents = 0;
  if (conn->sndoff < conn->sndlen) {
    ufd->events |= POLLOUT;
  }
  if (!rqwheel_isempty(&conn->reqs)) {
    ufd->events |= POLLIN;
  }

  return 1;
}


static inline void conn_update_eventq(conn_t *conn) {
  if (conn->ineventq && !conn_has_events(conn)) {
    TAILQ_REMOVE(&conn->reqs.th->eventq, conn, link);
    conn->ineventq = false;
  }
  else if (!conn->ineventq && conn_has_events(conn)) {
    TAILQ_INSERT_TAIL(&conn->reqs.th->eventq, conn, link);
    conn->ineventq = true;
  }
}


/* If there is data in conn.sndbuf, try to send it. Adjust send pointers. */
static void conn_flush(conn_t * conn) {
  int rv;

  if (conn->sndoff < conn->sndlen) {
    rv = write(conn->s, conn->sndbuf+conn->sndoff, conn->sndlen-conn->sndoff);
    if (rv < 0) {
      if (errno != EWOULDBLOCK)
        die("Failed to write to a TCP socket: %s\n", strerror(errno));
      return;
    }

    if (conn->sndoff == 0) {
      /* we just sent first byte of request into the socket. Update
         request send time in the request queue. */
      rqwheel_update_tsent(&conn->reqs);
    }

    conn->sndoff += rv;

    if (conn->sndoff >= conn->sndlen) {
      conn->sndoff = 0;
      conn->sndlen = 0;
      conn_update_eventq(conn);
    }
  }
  else {
    conn->sndoff = conn->sndlen = 0;
  }
}


/* Send a request of type t for key _k_ into _conn_. Returns 0
   if any part of the request was written into the socket, otherwise
   returns -1 and sets errno. errno is set to EWOULDBLOCK if the
   message could not be sent or buffered for sending. */
static int conn_send(conn_t *conn, reqtype_t t, int k) {
  int rv;

  conn_flush(conn);

  if (conn->sndlen > 0) {
    errno = EWOULDBLOCK;
    return -1;
  }

  /* at this point we do not have a message in .sndbuf */

  switch (t) {
  case req_get:
    conn->sndlen = compose_get(conn->sndbuf, conn->sndbufsize, k);
    break;
  case req_set:
    conn->sndlen = compose_set(conn->sndbuf, conn->sndbufsize, k);
    break;
  default:
    die("Invalid request type (%d) passed to conn_send()\n", t);
  }

  conn->sndoff = 0;

  rv = write(conn->s, conn->sndbuf, conn->sndlen);
  if (rv < 0) {
    if (errno != EWOULDBLOCK) {
      die("Failed to write to a TCP socket: %s\n", strerror(errno));
      return -1;
    }
    /* we haven't sent anything yet */
    conn->sndoff = 0;
  }
  else
    conn->sndoff = rv;

  rqwheel_append_request(&conn->reqs, t, k);

  conn_update_eventq(conn);

  return 0;
}



/* This function incrementally parses the contents of conn->rcvbuf right
   after _nread_ bytes have been appended to it. conn->rcvnext must not
   have been moved forward when this function is called. If one or more
   complete replies are identified, the function matches them up against
   the corresponding requests. It returns the number of complete replies
   identified, or 0 if no new complete replies have been found. */
static inline int conn_parse_stream(conn_t *conn, int nread) {
  int cnt=0;
  int n = conn->rcvnext;
  int nreplies = 0;

  /* search for a \n preceded by \r */
  for (cnt=0; cnt<nread; cnt++, n=succ(n, conn->rcvbufsize)) {
    if (conn->rcvbuf[n] == '\n' &&
        pre(n, conn->rcvbufsize) != conn->rcvstart &&
        conn->rcvbuf[pre(n, conn->rcvbufsize)] == '\r') {

      /* found a new \r\n */
      if (conn->nskipsep == 0) {
        /* This is the first separator in a new message. Try to identify
           if the message is a successful reply or an error message. */
        conn->rtype = -1;
        conn->rkey = -1;

        switch (conn->rcvbuf[conn->rcvstart]) {

        case 'C': /* CLIENT_ERROR */
          conn->reqs.th->stats[req_get].nfailed++;
          break;

        case 'E': /* ERROR, END with no value */
          conn->reqs.th->stats[req_get].nfailed++;
          break;

        case 'V': /* VALUE key flags bytes \r\n data \r\n END \r\n*/
          conn->rtype = req_get;
          /*TODO: conn->rkey = conn_read_key(conn);*/
          conn->nskipsep = 2;
          break;

        case 'S':  /* STORED, SERVER_ERROR */
          switch (conn->rcvbuf[succ(conn->rcvstart, conn->rcvbufsize)]) {
          case 'T': /* STORED */
            conn->rtype = req_set;
            break;
          case 'E': /* SERVER_ERROR */
            conn->reqs.th->stats[req_get].nfailed++;
            break;
          default:
            die("Got an invalid TCP reply from server: %c%c%c%c... "
                "expected STORED or SERVER_ERROR\n",
                conn->rcvbuf[conn->rcvstart],
                conn->rcvbuf[(conn->rcvstart+1)%conn->rcvbufsize],
                conn->rcvbuf[(conn->rcvstart+2)%conn->rcvbufsize],
                conn->rcvbuf[(conn->rcvstart+3)%conn->rcvbufsize]);
            break;
          }
          break;

        default:
          die("Got an invalid TCP reply from server: %c%c%c%c... ",
                conn->rcvbuf[conn->rcvstart],
                conn->rcvbuf[(conn->rcvstart+1)%conn->rcvbufsize],
                conn->rcvbuf[(conn->rcvstart+2)%conn->rcvbufsize],
                conn->rcvbuf[(conn->rcvstart+3)%conn->rcvbufsize]);
        } /* switch */
      }
      else {
        conn->nskipsep--;
      }

      if (conn->nskipsep == 0) {
        /* Got to the end of the current reply. */
        conn->rcvstart = succ(n, conn->rcvbufsize);
        rqwheel_note_tcp_reply(&conn->reqs, conn->rtype, conn->rkey);
        nreplies++;
      }
    }
  }

  conn->rcvnext = n;

  return nreplies;
}


/* This function attempts to read as many bytes as available from conn->s
   into conn->rcvbuf. conn->rcvbuf is a circular buffer sized to hold the
   largest valid reply. If any bytes have been read, the function starts
   or continues parsing the last reply read. If a complete reply is
   received and parsed, it is matched up with the corresponding request
   and the request queue and stats are updated. The function returns the
   number of replies that were fully parsed, 0 if no replies have been
   completed in this invocation, -1 with EWOULDBLOCK if no data was available
   in the socket receive buffer. All other socket errors and unexpected
   messages terminate the program. */
static inline int conn_recv(conn_t *conn) {
  struct iovec rcvbuf[2]; /* circular buffer represented as an iovec */
  int ioveclen; /* actual length of rcvbuf iovec */
  int nread; /* readv's return value */
  int nreplies;

  if (conn->rcvnext < conn->rcvstart) {
    rcvbuf[0].iov_base = conn->rcvbuf + conn->rcvnext;
    rcvbuf[0].iov_len = conn->rcvstart - conn->rcvnext;
    ioveclen = 1;
  }
  else {
    rcvbuf[0].iov_base = conn->rcvbuf + conn->rcvnext;
    rcvbuf[0].iov_len = conn->rcvbufsize - conn->rcvnext;
    rcvbuf[1].iov_base = conn->rcvbuf;
    rcvbuf[1].iov_len = conn->rcvstart;
    ioveclen = 2;
  }

  nread = readv(conn->s, rcvbuf, ioveclen);
  if (nread < 0) {
    if (errno != EWOULDBLOCK) {
      die("error while reading from a TCP connection: %s\n", strerror(errno));
    }
    return -1;
  }
  else if (nread == 0) {
    fprintf(stderr, "Server closed connection\n");
    stopthreads(0); /* stop all threads, print stats and exit */
    errno = EWOULDBLOCK;
    return -1;
  }

  /* read one or more bytes, go on parsing */

  nreplies = conn_parse_stream(conn, nread);

  if (nreplies > 0) {
    conn_update_eventq(conn);
  }

  return nreplies;
}


/* Compose a UDP request header (from libmcc/src/server.h). */
static inline void to_udp_header(char* buf,
                                 const uint32_t reqid, size_t reply_ports) {
  *buf++ = reqid >> 0x8;
  *buf++ = reqid & 0xff;
  *buf++ = 0;
  *buf++ = 0;
  *buf++ = 0;
  *buf++ = 1;
  *((uint16_t*)buf) = htons(reply_ports);
}


/* Try to parse the contents of NUL-terminated _dgram_ of size _len_
   bytes as a UDP reply to a 'get' request. Return UDP header through
   *udphdr. The function returns 1 if buffer contains a valid value, 0
   if the reply indicates that the key was not found, -1 if reply
   indicated an error. */
static inline int parse_udp_reply(const char *dgram, int len,
                                  udphdr_t *udphdr, int *key) {
  int rv;
  const char *reply = dgram+sizeof(udphdr_t);
  const char *sep;
  int bytes;

  udphdr->rqid = htons(((uint16_t*)dgram)[0]);
  udphdr->partno = htons(((uint16_t*)dgram)[1]);
  udphdr->nparts = htons(((uint16_t*)dgram)[2]);
  udphdr->reserved = 0;

  *key = -1;

  if (udphdr->partno != 0) {
    /* ignore parts other than the first */
    return 1;
  }

  sep = strchr(reply, '\r');
  if (!sep || *(sep+1) != '\n') {
    if (udphdr->nparts < 2)
      return -1;
    /* else assume that \r\n is in one of the parts to follow */
  }

  rv = sscanf(reply, "VALUE " KEYPREFIX "-%d %*d %d", key, &bytes);
  if (rv != 2) {
    if (memcmp(reply, "END", 3) == 0 ||
        memcmp(reply, "ERROR", 5) == 0 ||
        memcmp(reply, "CLIENT_ERROR", 12) == 0 ||
        memcmp(reply, "SERVER_ERROR", 12) == 0) {
      /* value was not found or an error occurred */
      return 0;
    }
    else {
      /* invalid reply */
      return -1;
    }
  }
  else if (bytes != valsz)
    return -1;

  return 1;
}


static void dgram_ap_init(dgram_ap_t *ap, int maxoutstanding, thread_t *th) {
  int rv;

  ap->s = socket(PF_INET, SOCK_DGRAM, 0);
  if (ap->s < 0)
    die("Failed to create UDP socket");

  set_nonblock(ap->s);

  if (socksndbufsz > 0) {
    setbufsize(ap->s, SO_SNDBUF, socksndbufsz);
  }

  ap->rcvbufsize = min(getbufsize(ap->s, SO_RCVBUF), (64*1024))+1024;
  ap->rcvbuf = (char*)malloc(ap->rcvbufsize);
  if (!ap->rcvbuf)
    die("Failed to malloc() %d bytes\n", ap->rcvbufsize);

  rqwheel_init(&ap->reqs, maxoutstanding, th);
}


static inline int dgram_ap_events(dgram_ap_t *ap, struct pollfd *ufd) {
  if (!rqwheel_isempty(&ap->reqs)) {
    ufd->fd = ap->s;
    ufd->events = POLLIN;
    ufd->revents = 0;
    return 1;
  }
  else
    return 0;
}


/* Send a GET request for a random key into _ap_. Returns 0 if the request
   was successfully sent, otherwise returns -1. errno is set to EWOULDBLOCK
   if the request could not  */
static inline int dgram_ap_send(dgram_ap_t *ap) {
  char buf[128];
  int dgsize;
  int rv;
  int k = random() % nkeys;

  to_udp_header(buf, ap->reqs.nextrqid, nreplyports);

  dgsize = compose_get(buf+8, sizeof(buf)-8, k) + 8;

  rv = sendto(ap->s, buf, dgsize, 0,
              (struct sockaddr*)&hostaddr_udp, sizeof(hostaddr_udp));
  if (rv < dgsize) {
    if (errno != EWOULDBLOCK)
      die("Failed to write to a UDP socket: %s\n", strerror(errno));
    return -1;
  }

  rqwheel_append_request(&ap->reqs, req_get, k);

  return 0;
}


/* Try to receive a datagram from ap->s. If a datagram is available,
   parse it and update the request wheel and statistics counters.
   Returns 0 if a datagram was read, otherwise -1. Sets errno to
   EWOULDBLOCK if no data was available on the socket. */
static inline int dgram_ap_recv(dgram_ap_t *ap) {
  int rv;
  int dglen;
  struct sockaddr_in from;
  socklen_t from_len = sizeof(from);
  udphdr_t udphdr;
  int key=-1;

  dglen = recvfrom(ap->s, ap->rcvbuf, ap->rcvbufsize, 0,
                   (struct sockaddr*)&from, &from_len);
  if (dglen < 0) {
    if (errno != EWOULDBLOCK) {
      die("recvfrom() failed: %s\n", strerror(errno));
    }
    return -1;
  }

  if (dglen <= sizeof(udphdr_t)) {
    /* a malformed reply */
    ap->reqs.th->stats[req_get].nbogus++;
    return 0;
  }

  ap->rcvbuf[dglen]='\0';

  rv = parse_udp_reply(ap->rcvbuf, dglen, &udphdr, &key);
  if (rv < 0) {
    ap->reqs.th->stats[req_get].nbogus++;
  }
  else {
    if (rv == 0) {
      ap->reqs.th->stats[req_get].nfailed++;
    }
    rqwheel_note_udp_reply(&ap->reqs, udphdr, key);
  }

  return 0;
}


/* This function completes the initialization of thread descriptor t. It is
   expected to execute on thread t itself. t->cpu must be already set. */
static void thread_init(thread_t *th) {
  int maxout_tcp; /* max # outstanding requests on each TCP connection */
  int i;

  for (i=0; i<reqtype_n; i++) {
    stats_init(&th->stats[i]);
  }

  TAILQ_INIT(&th->eventq);

  th->done = false;

  if (port_udp) { /* sets over TCP, gets over UDP */
    int maxout_udp; /* max # outstanding UDP GETs */

    if (nconns > 0) { /* will be sending TCP requests */
      maxout_tcp = max((float)rates[req_set]/nthreads, nkeys) *
                         RQWHEELCAP / nconns + 1024;
    }
    else
      maxout_tcp = 1024;

    maxout_udp = (float)rates[req_get]/nthreads * RQWHEELCAP + 1024;

    dgram_ap_init(&th->udp, maxout_udp, th);
  }
  else { /* all requests go over TCP */
    th->udp.s = -1;
    maxout_tcp = max((float)(rates[req_set]+rates[req_get])/nthreads, nkeys) *
                  RQWHEELCAP / nconns + 1024;
  }

  th->conns = (conn_t*)calloc(nconns, sizeof(conn_t));
  if (!th->conns) {
    die("calloc() failed to allocate %d connections for thread %d\n",
        nconns, th->cpu);
  }

  for (i=0; i<nconns; i++) {
    conn_init(th->conns+i, maxout_tcp, valsz+64, th);
  }
}


/* Block SIGINT and SIGALRM in the current thread. */
static void thread_block_signals(void) {
  sigset_t set;
  int rv;

  sigemptyset(&set);
  sigaddset(&set, SIGINT);
  sigaddset(&set, SIGALRM);

  rv = pthread_sigmask(SIG_BLOCK, &set, NULL);
  if (rv != 0) {
    die("pthread_sigmask() failed: %s\n", strerror(errno));
  }
}


typedef struct quantum_s {
  uint64_t current,   /* quantum number at current time */
           last,      /* last quantum number for which a request was
                         successfully sent */
           size;      /* quantum size in ticks */
} quantum_t;


static inline void quantum_init(quantum_t *q, uint64_t size) {
  q->current = q->last = 0;
  q->size = size;
}


static void thread_process_events(thread_t *th) {
  int nufds = 0; /* number of initialized entries in pfds */
  int nevents = 0; /* number of events to process */
  int rv, i;
  conn_t *c;

  struct pollfd *ufds = alloca(sizeof(struct pollfd)*(nconns+1));
  conn_t **connids = alloca(sizeof(conn_t*)*(nconns+1));
    /* connids[i] points to a connection C such whose socket is in ufds[i],
       connids[i]==NULL iff ufds[i] has events for th->udp */

  TAILQ_FOREACH(c, &th->eventq, link) {
    rv = conn_events(c, ufds+nufds);
    if (rv > 0) {
      connids[nufds] = c;
      nufds++;
    }
  }

  if (th->udp.s >= 0) {
    rv = dgram_ap_events(&th->udp, ufds+nufds);
    if (rv > 0) {
      connids[nufds] = NULL;
      nufds++;
    }
  }

  if (nufds == 0)
    return;

  if (nufds <= 2) {
    /* this is an optimization: if we have just 1 or 2 sockets,
       do not poll(), assume that their events fired. This saves one call
       to poll() per iteration. We expect that poll() to report the
       events anyway if we are sending at a high rate. */
    for (i=0; i<nufds; i++) {
      ufds[i].revents = ufds[i].events;
    }
    nevents = nufds;
  }
  else {
    /*DEBUG*/th->nufds+=nufds;
    /*DEBUG*/th->npolls++;
    nevents = poll(ufds, nufds, 0);
    if (nevents < 0) {
      if (errno == EINTR) {
        return;
      }
      die("poll() failed: %s\n", strerror(errno));
    }
  }

  if (nevents <= 0)
    return;

  for (i=0; i<nufds; i++) {
    if (connids[i]) {
      /* connection events */
      if (ufds[i].revents & POLLOUT) {
        conn_flush(connids[i]);
      }
      if (ufds[i].revents & POLLIN) {
        conn_recv(connids[i]);
      }
    }
    else {
      /* udp socket events */
      assert(ufds[i].fd == th->udp.s);

      if (ufds[i].revents & POLLIN) {
        dgram_ap_recv(&th->udp);
      }
    }
  }

}


static int thread_init_keys(thread_t *th) {
  quantum_t q;
  int initkey=0; /* next key to send an initialization 'set' for */
  int nextconn=0; /* connection to send next TCP request into */
  int rv;
  uint64_t tstart = cycle_timer();


  /* If sets are requested, initialization is done at the rate specified
     for sets. Otherwise it goes at the rate of gets. */
  if (rates[req_set] > 0) {
    quantum_init(&q, th->cpufreq * 1000000 * nthreads / rates[req_set]);
  }
  else { /* rates[req_get] > 0 */
    quantum_init(&q, th->cpufreq * 1000000 * nthreads / rates[req_get]);
  }

  while (th->stats[req_set].nmeasured < nkeys) {
    q.current = (cycle_timer() - tstart) / q.size;

    if (q.last < q.current) {
      rv = conn_send(&th->conns[nextconn], req_set, initkey);
      if (rv == 0) { /* wrote a complete or partial message into socket */
        initkey = succ(initkey, nkeys);
        q.last++;
      }
      nextconn = succ(nextconn, nconns);
    }

    thread_process_events(th);

    if (th->done)
      return -1;
  }

  return 0;
}


static void *thread_main(void *arg) {
  thread_t *th = (thread_t*)arg;
  quantum_t q[reqtype_n]; /* send quantum descriptors for gets and sets */

  int nextconn=0; /* connection to send next TCP request into */
  int rv, i;
  reqtype_t t;

  thread_init(th);

  thread_block_signals();

  bind_thread_to_cpu(th->cpu);
  th->cpufreq = get_cpu_frequency();

  /* The main loop spins on poll(), trying to send one get/set every
     q[req_(get|set)].size ticks. If a udp_port was specified, all gets
     go over UDP, otherwise they, along with all the sets, are sent into
     the first TCP connection that has space in its socket sendbuf. */

  if (rates[req_get] > 0 && port_tcp) {
    /* initialize keys if (1) we are going to send get requests and
       (2) we will have a TCP connection to send initial sets through. */
    rv = thread_init_keys(th);
    if (rv != 0) {
      th->tend = cycle_timer();
      return NULL;
    }
  }

  /* done with initialization, on to the main loop */

  th->tstart = cycle_timer();

  for (t=0; t<reqtype_n; t++) {
    quantum_init(&q[t], ((rates[t] > 0) ?
                         th->cpufreq * 1000000 * nthreads / rates[t] : 0));
  }

  do {
    for (t=0; t<reqtype_n; t++) {
      if (q[t].size > 0) {
        q[t].current = (cycle_timer() - th->tstart) / q[t].size;
        if (q[t].last < q[t].current) {
          if (t == req_get && th->udp.s >= 0)
            rv = dgram_ap_send(&th->udp);
          else {
            rv = conn_send(&th->conns[nextconn], t, random()%nkeys);
            nextconn = succ(nextconn, nconns);
          }
          if (rv == 0)
            q[t].last++;
        }
      }
    }

    thread_process_events(th);

  } while (!th->done);

  th->tend = cycle_timer();

  return NULL;
}


int usage(void) {
  printf("mcblaster: a memcached load generator.\n"
"\n"
"Usage: mcblast [options] <memcached host>\n"
"\n"
" Options:\n"
"  -p <port>         memcached TCP port\n"
"  -u <port>         memcached UDP port, if set all gets are over UDP\n"
"  -c N              open N TCP connections per thread (default 1)\n"
"  -d N              run for N seconds, by default run until SIGINT\n"
"  -k N              operate on N keys (default 1)\n"
"  -n                turn OFF tcp nodelay (default is ON)\n"
"  -q                suppress warnings\n"
"  -r N              try to send N gets per second (default 0)\n"
"  -s N              set socket send buffers to N bytes\n"
"  -t N              start N threads, default and max is as many as cores\n"
"  -w N              try to send N sets per second (default 0)\n"
"  -x N              the number of UDP reply ports (default 0)\n"
"  -z N              value size in bytes, default 100\n"
"\n");

  return 0;
}


void print_stats(void) {
  stats_t totals[reqtype_n];
  int n, i;
  reqtype_t t;
  uint64_t elapsed_usec=0; /* elapsed time in microseconds */
  double cpufreq=0; /* average cpu frequency for all threads */

  for (t=0; t<reqtype_n; t++) {
    stats_init(&totals[t]);
    for (n=0; n<nthreads; n++) {
      totals[t].rtt_total += threads[n].stats[t].rtt_total;
      if (totals[t].rtt_min > threads[n].stats[t].rtt_min) {
        totals[t].rtt_min = threads[n].stats[t].rtt_min;
      }
      if (totals[t].rtt_max < threads[n].stats[t].rtt_max) {
        totals[t].rtt_max = threads[n].stats[t].rtt_max;
      }
      totals[t].nmeasured += threads[n].stats[t].nmeasured;
      totals[t].nslow += threads[n].stats[t].nslow;
      totals[t].ntimedout += threads[n].stats[t].ntimedout;
      totals[t].nsent += threads[n].stats[t].nsent;
      totals[t].nfailed += threads[n].stats[t].nfailed;
      totals[t].nbogus += threads[n].stats[t].nbogus;

      for (i=0;
           i<sizeof(totals[t].rtt_buckets)/sizeof(totals[t].rtt_buckets[0]);
           i++) {
        totals[t].rtt_buckets[i] += threads[n].stats[t].rtt_buckets[i];
      }
    }
  }

  for (n=0; n<nthreads; n++) {
    cpufreq += threads[n].cpufreq;
    elapsed_usec += (threads[n].tend - threads[n].tstart) / threads[n].cpufreq;
  }
  elapsed_usec /= nthreads;
  cpufreq /= nthreads;

  for (t=0; t<reqtype_n; t++) {
    if (totals[t].nsent > 0) {
      printf("\n\
Request type   : %s\n\
Requests sent  : %lu\n\
Rate per second: %.0f\n\
Measured RTTs  : %lu\n\
RTT min/avg/max: %lu/%lu/%lu usec\n\
Timeouts       : %lu\n\
Errors         : %lu\n\
Invalid replies: %lu\n",
             reqtype_str[t],
             totals[t].nsent,
             (double)totals[t].nsent*1000000/elapsed_usec,
             totals[t].nmeasured,
             (uint64_t)(totals[t].rtt_min/cpufreq),
             (uint64_t)(totals[t].rtt_total/totals[t].nmeasured/cpufreq),
             (uint64_t)(totals[t].rtt_max/cpufreq),
             totals[t].ntimedout,
             totals[t].nfailed,
             totals[t].nbogus);
    }
  }

  for (t=0; t<reqtype_n; t++) {
    if (totals[t].nmeasured > 0) {
      printf("\nRTT distribution for '%s' requests:\n", reqtype_str[t]);
      for (i=0; i<MAXRTT/RTTBUCKET; i++) {
        if (totals[t].rtt_buckets[i] > 0) {
          printf("[%3d-%3d]\t: %lu\n", i*RTTBUCKET, (i+1)*RTTBUCKET,
                 totals[t].rtt_buckets[i]);
        }
      }
      printf("Over %6d usec: %lu\n", MAXRTT, totals[t].nslow);
    }
  }

  /*DEBUG*/
  printf("\n%.2f pollfd structs per poll()\n",
         (double)threads[0].nufds/threads[0].npolls);
}


int main(int argc, char *argv[]) {
  char opt;
  int i;
  int rv;
  in_addr_t hostaddr_in;
  pthread_attr_t tattr;
  int maxthreads;

  if (argc < 2)
    return usage();

  maxthreads = sysconf(_SC_NPROCESSORS_ONLN);
  if (maxthreads <= 0) {
    die("Invalid return result from sysconf(_SC_NPROCESSORS_ONLN): %d\n",
        maxthreads);
  }

  while ((opt=getopt(argc, argv, "p:u:c:d:k:nqr:s:t:w:x:z:")) != EOF) {
    switch (opt) {

    case 'p':
      port_tcp = atoi(optarg);
      if (port_tcp <= 0) {
        die("Invalid TCP port: %s\n", optarg);
      }
      break;

    case 'u':
      port_udp = atoi(optarg);
      if (port_udp <= 0) {
        die("Invalid UDP port: %s\n", optarg);
      }
      break;

    case 'c':
      nconns = atoi(optarg);
      if (nconns <= 0) {
        die("Invalid number of connections: %s\n", optarg);
      }
      break;

    case 'd':
      duration = atoi(optarg);
      if (duration <= 0) {
        die("Invalid time to run: %s\n", optarg);
      }
      break;

    case 'k':
      nkeys = atoi(optarg);
      if (nkeys <= 0 || nkeys > MAXKEYS) {
        die("Invalid number of keys: %s. The number must be "
            "between 1 and %d\n", optarg, MAXKEYS);
      }
      break;

    case 'n':
      nodelay = false;
      break;

    case 'q':
      quiet = true;
      break;

    case 'r':
      rates[req_get] = atoi(optarg);
      if (rates[req_get] <= 0) {
        die("Invalid number of requests per second: %s\n", optarg);
      }
      break;

    case 's':
      socksndbufsz = atoi(optarg);
      if (socksndbufsz <= 0) {
        die("Invalid socket sendbuf size: %s\n", optarg);
      }
      break;

    case 't':
      nthreads = atoi(optarg);
      if (nthreads <= 0 || nthreads > maxthreads) {
        die("Invalid number of threads specified. The number must be between "
            "1 and the number of cores on this machine (%d)\n", maxthreads);
      }
      break;

    case 'w':
      rates[req_set] = atoi(optarg);
      if (rates[req_set] <= 0) {
        die("Invalid number of requests per second: %s\n", optarg);
      }
      break;

    case 'x':
      nreplyports = atoi(optarg);
      if (nreplyports <= 0) {
        die("Invalid number of UDP reply ports specified: %s\n", optarg);
      }
      break;

    case 'z':
      valsz = atoi(optarg);
      if (valsz <= 0 || valsz > MAXSIZE) {
        die("Invalid key value size: %s. Size must be between 1 "
            "and %d bytes\n", optarg, MAXSIZE);
      }
      break;

    case '?':
      die("Unknown option -%c\n", optopt);
    }

  }

  if (optind >= argc) {
    die("Missing hostname. Run with no arguments for usage.\n");
    return 0;
  }

  hostname = argv[optind];

  if (rates[req_set] > 0 && port_tcp == 0) {
    die("Must specify a TCP port for sets (-p)\n");
  }

  if (rates[req_get] > 0 && port_tcp == 0 && port_udp == 0) {
    die("Must specify a port for gets (-p or -u)\n");
  }

  if (rates[req_set] == 0 && rates[req_get] == 0) {
    die("Must specify set rate (-w) or get rate (-r) or both.\n");
  }

  signal(SIGPIPE, SIG_IGN);

  hostaddr_in = host_to_addr(hostname, hostaddr_str);
  if (!hostaddr_in) {
    die("Hostname lookup failed for %s\n", hostname);
  }

  hostaddr.s_addr = hostaddr_in;

  if (port_udp > 0) {
    hostaddr_udp.sin_family = AF_INET;
    hostaddr_udp.sin_addr.s_addr = hostaddr.s_addr;
    hostaddr_udp.sin_port = htons(port_udp);
  }

  if (port_tcp > 0) {
    hostaddr_tcp.sin_family = AF_INET;
    hostaddr_tcp.sin_addr.s_addr = hostaddr.s_addr;
    hostaddr_tcp.sin_port = htons(port_tcp);
  }
  else /* no TCP connections */
    nconns = 0;

  srandom((unsigned int)cycle_timer());

  if (nthreads <= 0)
    nthreads = maxthreads;

  threads = (thread_t*)calloc(nthreads, sizeof(thread_t));
  if (!threads) {
    die("calloc() failed while creating threads\n");
  }

  rv = pthread_attr_setstacksize(&tattr, STACKSIZE);
  if (rv != 0) {
    die("pthread_attr_setstacksize() failed to set thread stack size to %d\n",
        STACKSIZE);
  }

  for (i=0; i<nthreads; i++) {
    threads[i].cpu = i;
    rv = pthread_create(&threads[i].pt, &tattr, thread_main, threads+i);
    if (rv != 0)
      die("pthread_create() failed with %d for thread #%i\n", i, errno);
  }

  signal(SIGINT, stopthreads);
  signal(SIGALRM, stopthreads);

  if (duration > 0)
    alarm(duration);

  for (i=0; i<nthreads; i++) {
    rv = pthread_join(threads[i].pt, NULL);
    if (rv != 0)
      die("pthread_join() failed with %d for thread #%i\n", i, errno);
  }

  print_stats();

  return 0;
}
