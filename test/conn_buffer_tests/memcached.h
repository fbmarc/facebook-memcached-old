#if !defined(_dummy_memcached_h_)
#define _dummy_memcached_h_

#include "generic.h"

#include <assert.h>

struct stats_s {
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

};

extern size_t append_to_buffer(char* const buffer_start,
                               const size_t buffer_size,
                               const size_t buffer_off,
                               const size_t reserved,
                               const char* fmt,
                               ...);
#define conn_buffer_reclamation do_conn_buffer_reclamation

#define V_LPRINTF(min_verbosity, string, ...)                           \
  if (verbose >= min_verbosity) {                                       \
    fprintf(stdout, "  %*s",                                            \
            min_verbosity, "");                                         \
    fprintf(stdout, string, ##__VA_ARGS__);                             \
  }                                                                     \

#define V_PRINTF(min_verbosity, string, ...) if (verbose >= min_verbosity) fprintf(stdout, string, ##__VA_ARGS__)
#define V_FLUSH(min_verbosity) if (verbose >= min_verbosity) fflush(stdout)

#if defined(DEBUG)
#define TASSERT(expr, ...) if (! (expr)) { if (verbose) { printf("assertion failed(%d): %s", __LINE__, #expr); printf("\n" __VA_ARGS__); } assert(0); }
#else
#define TASSERT(expr, ...) if (! (expr)) { if (verbose) { printf("assertion failed(%d): %s", __LINE__, #expr); printf("\n" __VA_ARGS__); } return 1; }
#endif /* #if defined(DEBUG) */


#if defined(INIT_TEST)
#include "init_test.h"
#endif /* #if defined(INIT_TEST) */

/* declaration of tests. */
typedef int (*tester_f)(int);
typedef struct {
    tester_f func;
    int is_fast;
} tester_info_t;

#define STATS_LOCK() ;
#define STATS_UNLOCK() ;
extern struct stats_s stats;

#include "memory_pool.h"

#endif /* #if !defined(_dummy_memcached_h_) */
