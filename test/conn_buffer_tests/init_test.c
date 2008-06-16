/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdlib.h>

#include "memcached.h"
#include "conn_buffer.h"

typedef enum test_status_e test_status_t;
enum test_status_e {
    PRE_INTERRUPT,
    POST_INTERRUPT,
};

static volatile test_status_t test_complete;


static int
initialized_test(int verbose) {
    V_LPRINTF(1, "%s\n", __FUNCTION__);

    conn_buffer_init(0, 0, 0, 0);
    TASSERT(cbs.initialized == true);

    return 0;
}


tester_info_t tests[] = {
    {initialized_test, 1},
};


int
main(int argc, char* argv[]) {
    int retval;
    int verbose = 0;
    int i;
    int limit_fast = 0;

    if (argc >= 2) {
        verbose = atoi(argv[1]);
    }
    if (argc >= 3) {
        limit_fast = 1;
    }

    for (i = 0; i < sizeof(tests) / sizeof(tester_info_t); i ++) {
        if (tests[i].is_fast == 0 && limit_fast == 1) {
            continue;
        }
        if ((retval = tests[i].func(verbose)) != 0) {
            return retval;
        }
    }

    return 0;
}

