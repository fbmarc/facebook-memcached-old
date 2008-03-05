/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/**
 * this file defines types that are used in multiple places, yet don't belong to
 * any one particular module.
 */

#if !defined(_generic_h_)
#define _generic_h_

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

/** Time relative to server start. Smaller than time_t on 64-bit systems. */
typedef unsigned int rel_time_t;

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

#if __WORDSIZE == 64
#define PRINTF_INT64_MODIFIER "l"
#else
#define PRINTF_INT64_MODIFIER "ll"
#endif

/* 
 * IOV_MAX comes from stdio.h.  make sure we include it before we try to define
 * it ourselves. 
 */
/* need this to get IOV_MAX on some platforms. */
#ifndef __need_IOV_MAX
#define __need_IOV_MAX
#endif

#include <stdio.h>

/* FreeBSD 4.x doesn't have IOV_MAX exposed. */
#ifndef IOV_MAX
#if defined(__FreeBSD__)
# define IOV_MAX 1024
#endif
#endif

#endif /* #if !defined(_generic_h_) */
