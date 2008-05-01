#if !defined(__main_h__)
#define __main_h__
int
main(int argc, char* argv[]) {
    int retval;
    int verbose = 0;
    int i;
    int limit_fast = 0;

    item_init();
    flat_storage_init(TOTAL_MEMORY);

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
#endif /* #if !defined(__main_h__) */
