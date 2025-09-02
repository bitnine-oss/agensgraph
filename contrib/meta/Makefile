# contrib/meta/Makefile

MODULE_big = meta
OBJS = \
	$(WIN32RES) \
	meta.o

EXTENSION = meta
DATA = meta--1.0.sql
PGFILEDESC = "meta - Utility functions for agensgraph"

REGRESS = meta

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/meta
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
