/**
 * This source file is used to print out a stack-trace when your program
 * segfaults.  It is relatively reliable and spot-on accurate.
 *
 * This code is in the public domain.  Use it as you see fit, some credit
 * would be appreciated, but is not a prerequisite for usage.  Feedback
 * on it's use would encourage further development and maintenance.
 *
 * Author:  Jaco Kroon <jaco@kroon.co.za>
 *
 * Copyright (C) 2005 - 2006 Jaco Kroon
 */
#define _GNU_SOURCE
#include <memory.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <ucontext.h>
#include <dlfcn.h>
#if defined(HAVE_EXECINFO_H)
#include <execinfo.h>
#endif /* #if defined(HAVE_EXECINFO_H) */
#ifndef NO_CPP_DEMANGLE
#include <cxxabi.h>
#endif
#include <syslog.h>

#if defined(REG_RIP)
# define SIGSEGV_STACK_IA64
# define REGFORMAT "%016lx"
#elif defined(REG_EIP)
# define SIGSEGV_STACK_X86
# define REGFORMAT "%08x"
#else
# define SIGSEGV_STACK_GENERIC
# define REGFORMAT "%lx"
#endif

static void signal_segv(int signum, siginfo_t* info, void*ptr) {
    size_t i;
#if defined(NGREG)
    ucontext_t *ucontext = (ucontext_t*)ptr;
#endif /* #if defined(NGREG) */

#if defined(SIGSEGV_STACK_X86) || defined(SIGSEGV_STACK_IA64)
    int f = 0;
    Dl_info dlinfo;
    void **bp = 0;
    void *ip = 0;
#endif
#if defined(HAVE_EXECINFO_H)
    void *bt[20];
    char **strings;
    size_t sz;
#endif /* #if defined(HAVE_EXECINFO_H) */

    openlog("memcached", LOG_PID, LOG_LOCAL0);

    syslog(LOG_ALERT, "Segmentation Fault!\n");
    syslog(LOG_ALERT, "info.si_signo = %d\n", signum);
    syslog(LOG_ALERT, "info.si_errno = %d\n", info->si_errno);
    syslog(LOG_ALERT, "info.si_code  = %d\n", info->si_code);
    syslog(LOG_ALERT, "info.si_addr  = %p\n", info->si_addr);
#if defined(NGREG)
    for(i = 0; i < NGREG; i++)
        syslog(LOG_ALERT, "reg[%02lu]       = 0x" REGFORMAT "\n", i, ucontext->uc_mcontext.gregs[i]);
#endif /* #if defined(NGREG) */

#if defined(SIGSEGV_STACK_X86) || defined(SIGSEGV_STACK_IA64)
# if defined(SIGSEGV_STACK_IA64)
    ip = (void*)ucontext->uc_mcontext.gregs[REG_RIP];
    bp = (void**)ucontext->uc_mcontext.gregs[REG_RBP];
# elif defined(SIGSEGV_STACK_X86)
    ip = (void*)ucontext->uc_mcontext.gregs[REG_EIP];
    bp = (void**)ucontext->uc_mcontext.gregs[REG_EBP];
# endif

    syslog(LOG_ALERT, "Stack trace:\n");
    while(ip) {
        if(!dladdr(ip, &dlinfo))
            break;

        const char *symname = dlinfo.dli_sname;
#ifndef NO_CPP_DEMANGLE
        int status;
        char *tmp = __cxa_demangle(symname, NULL, 0, &status);

        if(status == 0 && tmp)
            symname = tmp;
#endif

        syslog(LOG_ALERT, "% 2d: %p <%s+%u> (%s)\n",
                ++f,
                ip,
                symname,
                (unsigned)(ip - dlinfo.dli_saddr),
                dlinfo.dli_fname);

#ifndef NO_CPP_DEMANGLE
        if(tmp)
            free(tmp);
#endif

        if (bp == NULL) {
          break;
        }

        if(dlinfo.dli_sname && !strcmp(dlinfo.dli_sname, "main"))
            break;

        ip = bp[1];
        bp = (void**)bp[0];
    }
#endif
    syslog(LOG_ALERT, "End of stack trace\n");
    closelog();

#if defined(HAVE_EXECINFO_H)
    openlog("memcached", LOG_PID, LOG_LOCAL0);
    syslog(LOG_ALERT, "Stack trace (non-dedicated):\n");
    sz = backtrace(bt, 20);
    strings = backtrace_symbols(bt, sz);

    for(i = 0; i < sz; ++i)
        syslog(LOG_ALERT, "%s\n", strings[i]);

    syslog(LOG_ALERT, "End of stack trace (non-dedicated)\n");
    closelog();
#endif /* #if defined(HAVE_EXECINFO_H) */

    (void) i;                           /* consume i so we don't get a compiler warning. */

    exit (-1);
}

int setup_sigsegv() {
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_sigaction = signal_segv;
    action.sa_flags = SA_SIGINFO;
    if(sigaction(SIGABRT, &action, NULL) < 0) {
        perror("sigaction");
        return 0;
    }
    if(sigaction(SIGBUS, &action, NULL) < 0) {
        perror("sigaction");
        return 0;
    }
    if(sigaction(SIGFPE, &action, NULL) < 0) {
        perror("sigaction");
        return 0;
    }
    if(sigaction(SIGILL, &action, NULL) < 0) {
        perror("sigaction");
        return 0;
    }
    if(sigaction(SIGSEGV, &action, NULL) < 0) {
        perror("sigaction");
        return 0;
    }

    return 1;
}

#if 0
#ifndef SIGSEGV_NO_AUTO_INIT
static void __attribute((constructor)) init(void) {
    setup_sigsegv();
}
#endif
#endif /* #if 0 */
