bin_PROGRAMS = memcached memcached-debug

memcached_SOURCES = memcached.c slabs.c slabs.h \
	slabs_items.c slabs_items.h assoc.c assoc.h memcached.h \
	thread.c stats.c stats.h binary_sm.c binary_sm.h binary_protocol.h generic.h \
	items.h flat_storage.c flat_storage.h flat_storage_support.h \
        sigseg.c sigseg.h conn_buffer.c conn_buffer.h \
	memory_pool.h memory_pool_classes.h
memcached_debug_SOURCES = $(memcached_SOURCES)
memcached_CFLAGS = -Wall -Werror -Wno-deprecated-declarations
memcached_CPPFLAGS = -DNDEBUG -DNO_CPP_DEMANGLE
memcached_LDADD = @LIBOBJS@
memcached_LDFLAGS = -rdynamic
memcached_debug_CFLAGS = $(memcached_CFLAGS)
memcached_debug_CPPFLAGS = -DNO_CPP_DEMANGLE
memcached_debug_LDADD = $(memcached_LDADD)
memcached_debug_LDFLAGS = $(memcached_LDFLAGS)

SUBDIRS = doc
DIST_DIRS = scripts
EXTRA_DIST = doc scripts TODO t memcached.spec

test:	memcached-debug
	prove t

dist-hook:
	rm -rf $(distdir)/doc/.svn/
	rm -rf $(distdir)/scripts/.svn/
	rm -rf $(distdir)/t/.svn/
	rm -rf $(distdir)/t/lib/.svn/
