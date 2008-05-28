AC_PREREQ(2.52)
AC_INIT(memcached, 1.2.3, brad@danga.com)
AC_CANONICAL_SYSTEM
AC_CONFIG_SRCDIR(memcached.c)
AM_INIT_AUTOMAKE(AC_PACKAGE_NAME, AC_PACKAGE_VERSION)
AM_CONFIG_HEADER(config.h)

AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_INSTALL

trylibeventdir=""
AC_ARG_WITH(libevent,
       [  --with-libevent=PATH     Specify path to libevent installation ],
       [
                if test "x$withval" != "xno" ; then
                        trylibeventdir=$withval
                fi
       ]
)

dnl ------------------------------------------------------
dnl libevent detection.  swiped from Tor.  modified a bit.

LIBEVENT_URL=http://www.monkey.org/~provos/libevent/

AC_CACHE_CHECK([for libevent directory], ac_cv_libevent_dir, [
  saved_LIBS="$LIBS"
  saved_LDFLAGS="$LDFLAGS"
  saved_CPPFLAGS="$CPPFLAGS"
  le_found=no
  for ledir in $trylibeventdir "" $prefix /usr/local ; do
    LDFLAGS="$saved_LDFLAGS"
    LIBS="$saved_LIBS -levent"

    # Skip the directory if it isn't there.
    if test ! -z "$ledir" -a ! -d "$ledir" ; then
       continue;
    fi
    if test ! -z "$ledir" ; then
      if test -d "$ledir/lib" ; then
        LDFLAGS="-L$ledir/lib $LDFLAGS"
      else
        LDFLAGS="-L$ledir $LDFLAGS"
      fi
      if test -d "$ledir/include" ; then
        CPPFLAGS="-I$ledir/include $CPPFLAGS"
      else
        CPPFLAGS="-I$ledir $CPPFLAGS"
      fi
    fi
    # Can I compile and link it?
    AC_TRY_LINK([#include <sys/time.h>
#include <sys/types.h>
#include <event.h>], [ event_init(); ],
       [ libevent_linked=yes ], [ libevent_linked=no ])
    if test $libevent_linked = yes; then
       if test ! -z "$ledir" ; then
         ac_cv_libevent_dir=$ledir
       else
         ac_cv_libevent_dir="(system)"
       fi
       le_found=yes
       break
    fi
  done
  LIBS="$saved_LIBS"
  LDFLAGS="$saved_LDFLAGS"
  CPPFLAGS="$saved_CPPFLAGS"
  if test $le_found = no ; then
    AC_MSG_ERROR([libevent is required.  You can get it from $LIBEVENT_URL

      If it's already installed, specify its path using --with-libevent=/dir/
])
  fi
])
LIBS="$LIBS -levent"
if test $ac_cv_libevent_dir != "(system)"; then
  if test -d "$ac_cv_libevent_dir/lib" ; then
    LDFLAGS="-L$ac_cv_libevent_dir/lib $LDFLAGS"
    le_libdir="$ac_cv_libevent_dir/lib"
  else
    LDFLAGS="-L$ac_cv_libevent_dir $LDFLAGS"
    le_libdir="$ac_cv_libevent_dir"
  fi
  if test -d "$ac_cv_libevent_dir/include" ; then
    CPPFLAGS="-I$ac_cv_libevent_dir/include $CPPFLAGS"
  else
    CPPFLAGS="-I$ac_cv_libevent_dir $CPPFLAGS"
  fi
fi

dnl ----------------------------------------------------------------------------

AC_SEARCH_LIBS(socket, socket)
AC_SEARCH_LIBS(gethostbyname, nsl)
AC_SEARCH_LIBS(mallinfo, malloc)
AC_SEARCH_LIBS(pthread_create, pthread)

AC_CHECK_FUNC(daemon,AC_DEFINE([HAVE_DAEMON],,[Define this if you have daemon()]),[AC_LIBOBJ(daemon)])

AC_HEADER_STDBOOL
AC_C_CONST
AC_CHECK_HEADER(regex.h, AC_DEFINE(HAVE_REGEX_H,,[do we have regex.h?]))
AC_CHECK_HEADER(malloc.h, AC_DEFINE(HAVE_MALLOC_H,,[do we have malloc.h?]))
AC_CHECK_MEMBER([struct mallinfo.arena], [
        AC_DEFINE(HAVE_STRUCT_MALLINFO,,[do we have stuct mallinfo?])
    ], ,[
#    include <malloc.h>
    ]
)
AC_CHECK_HEADER(execinfo.h, AC_DEFINE(HAVE_EXECINFO_H,,[do we have execinfo.h?]))
AC_CHECK_HEADER(stdarg.h, AC_DEFINE(HAVE_STDARG_H,,[do we have stdarg.h?]))

dnl From licq: Copyright (c) 2000 Dirk Mueller
dnl Check if the type socklen_t is defined anywhere
AC_DEFUN([AC_C_SOCKLEN_T],
[AC_CACHE_CHECK(for socklen_t, ac_cv_c_socklen_t,
[
  AC_TRY_COMPILE([
    #include <sys/types.h>
    #include <sys/socket.h>
  ],[
    socklen_t foo;
  ],[
    ac_cv_c_socklen_t=yes
  ],[
    ac_cv_c_socklen_t=no
  ])
])
if test $ac_cv_c_socklen_t = no; then
  AC_DEFINE(socklen_t, int, [define to int if socklen_t not available])
fi
])

AC_C_SOCKLEN_T

dnl Check if we're a little-endian or a big-endian system, needed by hash code
AC_DEFUN([AC_C_ENDIAN],
[AC_CACHE_CHECK(for endianness, ac_cv_c_endian,
[
  AC_RUN_IFELSE(
    [AC_LANG_PROGRAM([], [dnl
        long val = 1;
        char *c = (char *) &val;
        exit(*c == 1);
    ])
  ],[
    ac_cv_c_endian=big
  ],[
    ac_cv_c_endian=little
  ])
])
if test $ac_cv_c_endian = big; then
  AC_DEFINE(ENDIAN_BIG, 1, [machine is bigendian])
fi
if test $ac_cv_c_endian = little; then
  AC_DEFINE(ENDIAN_LITTLE, 1, [machine is littleendian])
fi
])

AC_C_ENDIAN

dnl Check whether the user wants threads or not
AC_ARG_ENABLE(threads,
  [AS_HELP_STRING([--enable-threads],[support multithreaded execution])],
  [if test "$ac_cv_search_pthread_create" != "no"; then
    AC_DEFINE([USE_THREADS],,[Define this if you want to use pthreads])
   else
    AC_MSG_ERROR([Can't enable threads without the POSIX thread library.])
   fi])

dnl Check whether the user wants operation stats classified into buckets or not
AC_ARG_ENABLE(stat-buckets,
  [AS_HELP_STRING([--enable-stat-buckets],[enable operation stats classified into buckets])],
  [if test "$enableval" = "yes"; then
    AC_DEFINE([STATS_BUCKETS],,[Define this if you want operation stats classified into buckets])
   fi])

dnl Check whether the user wants cost-benefit stats to be collected.
AC_ARG_ENABLE(cost-benefit-stats,
  [AS_HELP_STRING([--enable-cost-benefit-stats],[enable cost-benefit stats])],
  [if test "$enableval" = "yes"; then
    AC_DEFINE([COST_BENEFIT_STATS],,[Define this if you want cost-benefit stats])
   fi])

dnl Enable multiple udp reply ports
AC_ARG_ENABLE(udp-reply-ports,
  [AS_HELP_STRING([--enable-udp-reply-ports],[enable multiple udp reply ports])],
  [if test "$enableval" = "yes"; then
    AC_DEFINE([HAVE_UDP_REPLY_PORTS],,[Define this if you want multiple udp reply ports])
   fi])

dnl Check whether the user wants the slab allocator or not
AC_ARG_ENABLE(slab_allocator,
        [AS_HELP_STRING([--enable-slab-allocator],[use the slab allocator (default=yes)])],
	[if test "$enableval" = "no"; then
             want_slab_allocator="no"
         else
             want_slab_allocator="yes"
         fi
        ],)

dnl Check whether the user wants the flat allocator or not
AC_ARG_ENABLE(flat_allocator,
        [AS_HELP_STRING([--enable-flat-allocator],[use the flat allocator (default=no)])],
	[if test "$enableval" = "no"; then
             want_flat_allocator="no"
         else
             want_flat_allocator="yes"
         fi
        ],)

[if test "x$want_slab_allocator" = "xyes" && test "x$want_flat_allocator" = "xyes"; then
    ]AC_MSG_ERROR([Cannot enable both the slab allocator and the flat allocator])[
fi]

[if test "x$want_slab_allocator" = "x" && test "x$want_flat_allocator" = "x"; then
    want_slab_allocator="yes"
fi]

[if test "x$want_slab_allocator" = "xyes"; then
    ]AC_DEFINE([USE_SLAB_ALLOCATOR],,[Define this if you want to use the slab allocator])[
elif test "x$want_flat_allocator" = "xyes"; then
    ]AC_DEFINE([USE_FLAT_ALLOCATOR],,[Define this if you want to use the flat allocator])[
fi]

AC_CHECK_FUNCS(mlockall)
AC_CHECK_LIB(dl, dladdr)
AC_CHECK_FUNCS(dladdr)

AC_CONFIG_FILES(Makefile doc/Makefile)
AC_OUTPUT
