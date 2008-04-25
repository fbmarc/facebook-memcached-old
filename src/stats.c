/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Detailed statistics management. For simple stats like total number of
 * "get" requests, we use inline code in memcached.c and friends, but when
 * stats detail mode is activated, the code here records more information.
 *
 * Author:
 *   Steven Grimm <sgrimm@facebook.com>
 *
 * $Id$
 */
#include "memcached.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/*
 * Stats are tracked on the basis of key prefixes. This is a simple
 * fixed-size hash of prefixes; we run the prefixes through the same
 * CRC function used by the cache hashtable.
 */
typedef struct _prefix_stats PREFIX_STATS;
struct _prefix_stats {
    char         *prefix;
    size_t        prefix_len;
    uint32_t      num_items;
    rel_time_t    last_update;
    uint64_t      num_gets;
    uint64_t      num_sets;
    uint64_t      num_deletes;
    uint64_t      num_hits;
    uint64_t      num_evicts;
    uint64_t      num_bytes;
    uint64_t      total_lifetime;
    PREFIX_STATS *next;
};

SIZE_BUCKETS set;
SIZE_BUCKETS get;
SIZE_BUCKETS evict;
SIZE_BUCKETS delete;
SIZE_BUCKETS overwrite;

#define BUCKETS_RANGE(start, end, skip)                                 \
    uint64_t   hits_ ## start ## _ ## end [ ((end-start) / skip) ];     \
    uint64_t   slot_seconds_ ## start ## _ ## end [ ((end-start) / skip) ]; \
    rel_time_t last_update_ ## start ## _ ## end [ ((end-start) / skip) ]; \
    uint32_t   slots_ ## start ## _ ## end [ ((end-start) / skip) ];
typedef struct cost_benefit_buckets_s cost_benefit_buckets_t;
struct cost_benefit_buckets_s {
#include "buckets.h"
};

static cost_benefit_buckets_t cb_buckets;

#define PREFIX_HASH_SIZE 256

static PREFIX_STATS *prefix_stats[PREFIX_HASH_SIZE];
static int num_prefixes = 0;
static int total_prefix_size = 0;
static PREFIX_STATS wildcard;

void stats_prefix_init() {
    memset(prefix_stats, 0, sizeof(prefix_stats));
    memset(&wildcard, 0, sizeof(PREFIX_STATS));
}

void stats_buckets_init(void) {
    memset(&set, 0, sizeof(set));
    memset(&get, 0, sizeof(get));
    memset(&evict, 0, sizeof(evict));
    memset(&delete, 0, sizeof(delete));
    memset(&overwrite, 0, sizeof(overwrite));
}

void stats_cost_benefit_init(void) {
    memset(&cb_buckets, 0, sizeof(cb_buckets));
}

/*
 * Cleans up all our previously collected stats. NOTE: the stats lock is
 * assumed to be held when this is called.
 */
void stats_prefix_clear() {
    int i;

    for (i = 0; i < PREFIX_HASH_SIZE; i++) {
        PREFIX_STATS *cur, *next;
        for (cur = prefix_stats[i]; cur != NULL; cur = next) {
            next = cur->next;
            free(cur->prefix);
            free(cur);
        }
        prefix_stats[i] = NULL;
    }
    num_prefixes = 0;
    total_prefix_size = 0;
    memset(&wildcard, 0, sizeof(PREFIX_STATS));
}

/*
 * Returns the stats structure for a prefix, creating it if it's not already
 * in the list.
 */
/*@null@*/
static PREFIX_STATS *stats_prefix_find(const char *key) {
    PREFIX_STATS *pfs;
    uint32_t hashval;
    size_t length;

    assert(key != NULL);

    for (length = 0; key[length] != '\0'; length++)
        if (key[length] == settings.prefix_delimiter)
            break;

    if (key[length] == '\0') {
        return &wildcard;
    }

    hashval = hash(key, length, 0) % PREFIX_HASH_SIZE;

    for (pfs = prefix_stats[hashval]; NULL != pfs; pfs = pfs->next) {
        if (strncmp(pfs->prefix, key, length) == 0)
            return pfs;
    }

    pfs = calloc(sizeof(PREFIX_STATS), 1);
    if (NULL == pfs) {
        perror("Can't allocate space for stats structure: calloc");
        return NULL;
    }

    pfs->prefix = malloc(length + 1);
    if (NULL == pfs->prefix) {
        perror("Can't allocate space for copy of prefix: malloc");
        free(pfs);
        return NULL;
    }

    strncpy(pfs->prefix, key, length);
    pfs->prefix[length] = '\0';      /* because strncpy() sucks */
    pfs->prefix_len = length;

    pfs->next = prefix_stats[hashval];
    prefix_stats[hashval] = pfs;

    num_prefixes++;
    total_prefix_size += length;

    return pfs;
}

/*
 * Records a "get" of a key.
 */
void stats_prefix_record_get(const char *key, const bool is_hit) {
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key);
    if (NULL != pfs) {
        pfs->num_gets++;
        if (is_hit) {
            pfs->num_hits++;
        }
    }
    STATS_UNLOCK();
}

/*
 * Records a "delete" of a key.
 */
void stats_prefix_record_delete(const char *key) {
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key);
    if (NULL != pfs) {
        pfs->num_deletes++;
    }
    STATS_UNLOCK();
}

/*
 * Records a "set" of a key.
 */
void stats_prefix_record_set(const char *key) {
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key);
    if (NULL != pfs) {
        pfs->num_sets++;

        /*
         * increment total lifetime to reflect time elapsed since last update.
         * item count cannot be incremented here because the set/add/replace may
         * fail.
         */
        pfs->total_lifetime += pfs->num_items * (current_time - pfs->last_update);
        pfs->last_update = current_time;
    }
    STATS_UNLOCK();
}

/*
 * Records the change in byte total due to a "set" of a key.
 */
void stats_prefix_record_byte_total_change(char *key, long bytes) {
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key);
    if (NULL != pfs) {
        pfs->num_bytes += bytes;

        /* increment item count. */
        pfs->num_items ++;
    }
    STATS_UNLOCK();
}

/*
 * Records a "removal" of a key.
 */
void stats_prefix_record_removal(char *key, size_t bytes, rel_time_t time, long flags) {
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key);
    if (NULL != pfs) {
        pfs->num_bytes-=bytes;
        if (flags & UNLINK_IS_EVICT) {
            pfs->num_evicts++;
        }

        /* increment total lifetime to reflect time elapsed since last update. */
        pfs->total_lifetime += pfs->num_items * (current_time - pfs->last_update);
        pfs->last_update = current_time;

        /* remove the lifetime of the object that we're booting out. */
        pfs->total_lifetime -= (current_time - time);

        /* increment item count. */
        pfs->num_items --;
    }
    STATS_UNLOCK();
}

/*
 * Returns stats in textual form suitable for writing to a client.
 */
/*@null@*/
char *stats_prefix_dump(int *length) {
    const char *format = "PREFIX %s item %u get %" PRINTF_INT64_MODIFIER \
        "u hit %" PRINTF_INT64_MODIFIER "u set %" PRINTF_INT64_MODIFIER \
        "u del %" PRINTF_INT64_MODIFIER "u evict %" PRINTF_INT64_MODIFIER \
        "u bytes %" PRINTF_INT64_MODIFIER "u lifetime %" PRINTF_INT64_MODIFIER "u\r\n";
    PREFIX_STATS *pfs;
    char *buf;
    int i;
    size_t size, offset = 0;
    uint64_t lifetime;
    const int format_len = sizeof("%" PRINTF_INT64_MODIFIER "u") - sizeof("");
    char terminator[] = "END\r\n";

    /*
     * Figure out how big the buffer needs to be. This is the sum of the
     * lengths of the prefixes themselves, plus the size of one copy of
     * the per-prefix output with 20-digit values for all the counts,
     * plus space for the "END" at the end.
     */
    STATS_LOCK();
    size = strlen(format) + total_prefix_size +
        (num_prefixes + 1) * (strlen(format) - 2 /* %s */
                              + 8 * (20 - format_len)) /* %llu replaced by 20-digit num */
        + sizeof("*wildcard*")
        + sizeof("END\r\n");
    buf = malloc(size);
    if (NULL == buf) {
        perror("Can't allocate stats response: malloc");
        STATS_UNLOCK();
        return NULL;
    }

    for (i = 0; i < PREFIX_HASH_SIZE; i++) {
        for (pfs = prefix_stats[i]; NULL != pfs; pfs = pfs->next) {
            /*
             * increment total lifetime to reflect time elapsed since last update.
             * item count cannot be incremented here because the set/add/replace may
             * fail.
             */
            pfs->total_lifetime += pfs->num_items * (current_time - pfs->last_update);
            pfs->last_update = current_time;

            if (pfs->num_items == 0) {
                lifetime = 0;
            } else {
                lifetime = pfs->total_lifetime / pfs->num_items;
            }

            offset = append_to_buffer(buf, size, offset, sizeof(terminator),
                                      format,
                                      pfs->prefix, pfs->num_items, pfs->num_gets, pfs->num_hits,
                                      pfs->num_sets, pfs->num_deletes, pfs->num_evicts,
                                      pfs->num_bytes, lifetime);
        }
    }

    /*
     * increment total lifetime to reflect time elapsed since last update.
     * item count cannot be incremented here because the set/add/replace may
     * fail.
     */
    wildcard.total_lifetime += wildcard.num_items * (current_time - wildcard.last_update);
    wildcard.last_update = current_time;

    if (wildcard.num_items == 0) {
        lifetime = 0;
    } else {
        lifetime = wildcard.total_lifetime / wildcard.num_items;
    }

    offset = append_to_buffer(buf, size, offset, sizeof(terminator),
                              format,
                              "*wildcard*", wildcard.num_items,  wildcard.num_gets, wildcard.num_hits,
                              wildcard.num_sets, wildcard.num_deletes, wildcard.num_evicts,
                              wildcard.num_bytes, lifetime);

    STATS_UNLOCK();
    offset = append_to_buffer(buf, size, offset, 0, terminator);

    *length = offset;

    return buf;
}

/** dumps out a list of objects of each size, with granularity of 32 bytes */
/*@null@*/
char* item_stats_buckets(int *bytes) {
    size_t bufsize = (2 * 1024 * 1024), offset = 0;
    char *buf = (char *)malloc(bufsize); /* 2MB max response size */
    int i, j;
    char terminator[] = "END\r\n";

    *bytes = 0;
    if (buf == 0) {
        return NULL;
    }

#if defined(STATS_BUCKETS)
    /* write the buffer */
#define BUCKETS_RANGE(start, end, skip)                                 \
    for (i = start, j = 0; i < end; i += skip, j ++) {                  \
        if (set.size_ ## start ## _ ## end[j] != 0 ||                   \
            get.size_ ## start ## _ ## end[j] != 0 ||                   \
            evict.size_ ## start ## _ ## end[j] != 0 ||                 \
            delete.size_ ## start ## _ ## end[j] != 0 ||                \
            overwrite.size_ ## start ## _ ## end[j] != 0) {             \
            offset = append_to_buffer(buf, bufsize, offset,             \
                                      sizeof(terminator),               \
                                      "%8d-%-8d:%16" PRINTF_INT64_MODIFIER \
                                      "u sets %16" PRINTF_INT64_MODIFIER \
                                      "u gets %16" PRINTF_INT64_MODIFIER \
                                      "u evicts %16" PRINTF_INT64_MODIFIER \
                                      "u deletes %16" PRINTF_INT64_MODIFIER \
                                      "u overwrites\r\n",               \
                                      i, i + skip - 1,                  \
                                      set.size_ ## start ## _ ## end[j], \
                                      get.size_ ## start ## _ ## end[j], \
                                      evict.size_ ## start ## _ ## end[j], \
                                      delete.size_ ## start ## _ ## end[j], \
                                      overwrite.size_ ## start ## _ ## end[j]); \
        }                                                               \
}
#include "buckets.h"
#else
    (void) i;
    (void) j;
#endif /* #if defined(STATS_BUCKETS) */

    offset = append_to_buffer(buf, bufsize, offset, 0, terminator);
    *bytes = offset;
    return buf;
}


#ifdef UNIT_TEST

/****************************************************************************
      To run unit tests, compile with $(CC) -DUNIT_TEST stats.c assoc.o
      (need assoc.o to get the hash() function).
****************************************************************************/

struct settings settings;

static char *current_test = "";
static int test_count = 0;
static int fail_count = 0;

static void fail(char *what) { printf("\tFAIL: %s\n", what); fflush(stdout); fail_count++; }
static void test_equals_int(char *what, int a, int b) { test_count++; if (a != b) fail(what); }
static void test_equals_ptr(char *what, void *a, void *b) { test_count++; if (a != b) fail(what); }
static void test_equals_str(char *what, const char *a, const char *b) { test_count++; if (strcmp(a, b)) fail(what); }
static void test_equals_ull(char *what, uint64_t a, uint64_t b) { test_count++; if (a != b) fail(what); }
static void test_notequals_ptr(char *what, void *a, void *b) { test_count++; if (a == b) fail(what); }
static void test_notnull_ptr(char *what, void *a) { test_count++; if (NULL == a) fail(what); }

static void test_prefix_find() {
    PREFIX_STATS *pfs1, *pfs2;

    pfs1 = stats_prefix_find("abc");
    test_notnull_ptr("initial prefix find", pfs1);
    test_equals_ull("request counts", 0ULL,
        pfs1->num_gets + pfs1->num_sets + pfs1->num_deletes + pfs1->num_hits);
    pfs2 = stats_prefix_find("abc");
    test_equals_ptr("find of same prefix", pfs1, pfs2);
    pfs2 = stats_prefix_find("abc:");
    test_equals_ptr("find of same prefix, ignoring delimiter", pfs1, pfs2);
    pfs2 = stats_prefix_find("abc:d");
    test_equals_ptr("find of same prefix, ignoring extra chars", pfs1, pfs2);
    pfs2 = stats_prefix_find("xyz123");
    test_notequals_ptr("find of different prefix", pfs1, pfs2);
    pfs2 = stats_prefix_find("ab:");
    test_notequals_ptr("find of shorter prefix", pfs1, pfs2);
}

static void test_prefix_record_get() {
    PREFIX_STATS *pfs;

    stats_prefix_record_get("abc:123", 0);
    pfs = stats_prefix_find("abc:123");
    test_equals_ull("get count after get #1", 1, pfs->num_gets);
    test_equals_ull("hit count after get #1", 0, pfs->num_hits);
    stats_prefix_record_get("abc:456", 0);
    test_equals_ull("get count after get #2", 2, pfs->num_gets);
    test_equals_ull("hit count after get #2", 0, pfs->num_hits);
    stats_prefix_record_get("abc:456", 1);
    test_equals_ull("get count after get #3", 3, pfs->num_gets);
    test_equals_ull("hit count after get #3", 1, pfs->num_hits);
    stats_prefix_record_get("def:", 1);
    test_equals_ull("get count after get #4", 3, pfs->num_gets);
    test_equals_ull("hit count after get #4", 1, pfs->num_hits);
}

static void test_prefix_record_delete() {
    PREFIX_STATS *pfs;

    stats_prefix_record_delete("abc:123");
    pfs = stats_prefix_find("abc:123");
    test_equals_ull("get count after delete #1", 0, pfs->num_gets);
    test_equals_ull("hit count after delete #1", 0, pfs->num_hits);
    test_equals_ull("delete count after delete #1", 1, pfs->num_deletes);
    test_equals_ull("set count after delete #1", 0, pfs->num_sets);
    stats_prefix_record_delete("def:");
    test_equals_ull("delete count after delete #2", 1, pfs->num_deletes);
}

static void test_prefix_record_set() {
    PREFIX_STATS *pfs;

    stats_prefix_record_set("abc:123");
    pfs = stats_prefix_find("abc:123");
    test_equals_ull("get count after set #1", 0, pfs->num_gets);
    test_equals_ull("hit count after set #1", 0, pfs->num_hits);
    test_equals_ull("delete count after set #1", 0, pfs->num_deletes);
    test_equals_ull("set count after set #1", 1, pfs->num_sets);
    stats_prefix_record_delete("def:");
    test_equals_ull("set count after set #2", 1, pfs->num_sets);
}

static void test_prefix_dump() {
    int hashval = hash("abc", 3, 0) % PREFIX_HASH_SIZE;
    char tmp[500];
    char *expected;
    int keynum;
    int length;

    test_equals_str("empty stats", "END\r\n", stats_prefix_dump(&length));
    test_equals_int("empty stats length", 5, length);
    stats_prefix_record_set("abc:123");
    expected = "PREFIX abc get 0 hit 0 set 1 del 0\r\nEND\r\n";
    test_equals_str("stats after set", expected, stats_prefix_dump(&length));
    test_equals_int("stats length after set", strlen(expected), length);
    stats_prefix_record_get("abc:123", 0);
    expected = "PREFIX abc get 1 hit 0 set 1 del 0\r\nEND\r\n";
    test_equals_str("stats after get #1", expected, stats_prefix_dump(&length));
    test_equals_int("stats length after get #1", strlen(expected), length);
    stats_prefix_record_get("abc:123", 1);
    expected = "PREFIX abc get 2 hit 1 set 1 del 0\r\nEND\r\n";
    test_equals_str("stats after get #2", expected, stats_prefix_dump(&length));
    test_equals_int("stats length after get #2", strlen(expected), length);
    stats_prefix_record_delete("abc:123");
    expected = "PREFIX abc get 2 hit 1 set 1 del 1\r\nEND\r\n";
    test_equals_str("stats after del #1", expected, stats_prefix_dump(&length));
    test_equals_int("stats length after del #1", strlen(expected), length);

    /* The order of results might change if we switch hash functions. */
    stats_prefix_record_delete("def:123");
    expected = "PREFIX abc get 2 hit 1 set 1 del 1\r\n"
               "PREFIX def get 0 hit 0 set 0 del 1\r\n"
               "END\r\n";
    test_equals_str("stats after del #2", expected, stats_prefix_dump(&length));
    test_equals_int("stats length after del #2", strlen(expected), length);

    /* Find a key that hashes to the same bucket as "abc" */
    for (keynum = 0; keynum < PREFIX_HASH_SIZE * 100; keynum++) {
        sprintf(tmp, "%d", keynum);
        if (hashval == hash(tmp, strlen(tmp), 0) % PREFIX_HASH_SIZE) {
            break;
        }
    }
    stats_prefix_record_set(tmp);
    sprintf(tmp, "PREFIX %d get 0 hit 0 set 1 del 0\r\n"
                 "PREFIX abc get 2 hit 1 set 1 del 1\r\n"
                 "PREFIX def get 0 hit 0 set 0 del 1\r\n"
                 "END\r\n", keynum);
    test_equals_str("stats with two stats in one bucket",
                    tmp, stats_prefix_dump(&length));
    test_equals_int("stats length with two stats in one bucket",
                    strlen(tmp), length);
}

static void run_test(char *what, void (*func)(void)) {
    current_test = what;
    test_count = fail_count = 0;
    puts(what);
    fflush(stdout);

    stats_prefix_clear();
    (func)();
    printf("\t%d / %d pass\n", (test_count - fail_count), test_count);
}

/* In case we're compiled in thread mode */
void mt_stats_lock() { }
void mt_stats_unlock() { }

main(int argc, char **argv) {
    stats_prefix_init();
    settings.prefix_delimiter = ':';
    run_test("stats_prefix_find", test_prefix_find);
    run_test("stats_prefix_record_get", test_prefix_record_get);
    run_test("stats_prefix_record_delete", test_prefix_record_delete);
    run_test("stats_prefix_record_set", test_prefix_record_set);
    run_test("stats_prefix_dump", test_prefix_dump);
}

#endif
