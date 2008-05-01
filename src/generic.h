/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/**
 * this file defines types that are used in multiple places, yet don't belong to
 * any one particular module.
 */

#if !defined(_generic_h_)
#define _generic_h_

/* this needs to go on top. */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

/**
 * Start including stuff to get type definitions.  Some will depend on config.h.
 */

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

/* some POSIX systems need the following definition
 * to get mlockall flags out of sys/mman.h.  */
#ifndef _P1003_1B_VISIBLE
#define _P1003_1B_VISIBLE
#endif
#include <sys/mman.h>

/**
 * Declare simple types (enums, typedefs of native types).
 */

#if __WORDSIZE == 64
#define PRINTF_INT64_MODIFIER "l"
#else
#define PRINTF_INT64_MODIFIER "ll"
#endif

/** Time relative to server start. Smaller than time_t on 64-bit systems. */
typedef unsigned int rel_time_t;


/**
 * Start including stuff to get other header files.
 */

#ifdef HAVE_MALLOC_H
/* OpenBSD has a malloc.h, but warns to use stdlib.h instead */
#ifndef __OpenBSD__
#include <malloc.h>
#endif
#endif

/* unistd.h is here */
#if HAVE_UNISTD_H
# include <unistd.h>
#endif

#ifdef HAVE_STRING_H
#include <string.h>
#endif

/* 
 * IOV_MAX comes from stdio.h.  Make sure we include it before we try to define
 * it ourselves. 
 */
/* need this to get IOV_MAX on some platforms. */
#ifndef __need_IOV_MAX
#define __need_IOV_MAX
#endif

#include <stdio.h>
#include <limits.h>

/* FreeBSD 4.x doesn't have IOV_MAX exposed. */
#ifndef IOV_MAX
#if defined(__FreeBSD__)
# define IOV_MAX 1024
#endif
#endif

#endif /* #if !defined(_generic_h_) */
