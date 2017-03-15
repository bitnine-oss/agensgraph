#
# pg_statsinfo: lib/Makefile
#
#    Copyright (c) 2009-2017, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#
SRCS = \
	libstatsinfo.c \
	last_xact_activity.c \
	pg_control.c \
	port.c \
	pgut/pgut-be.c \
	pgut/pgut-spi.c
OBJS = $(SRCS:.c=.o)
DATA_built = pg_statsinfo.sql
DATA = uninstall_pg_statsinfo.sql
MODULE_big = pg_statsinfo

SHLIB_LINK = -Wl,--version-script=library.map

ifndef USE_PGXS
top_builddir = ../../../..
makefile_global = $(top_builddir)/src/Makefile.global
ifeq "$(wildcard $(makefile_global))" ""
USE_PGXS = 1    # use pgxs if not in contrib directory
endif
endif

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/$(MODULE_big)
include $(makefile_global)
include $(top_srcdir)/contrib/contrib-global.mk
endif
