#
# pg_statsinfo: bin/Makefile
#
#    Copyright (c) 2009-2018, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#
SRCS = \
	pg_statsinfod.c \
	autovacuum.c \
	checkpoint.c \
	collector.c \
	logger.c \
	logger_send.c \
	logger_common.c \
	logger_in.c \
	logger_out.c \
	maintenance.c \
	snapshot.c \
	writer.c \
	pg_control.c \
	pgut/pgut.c \
	pgut/pgut-list.c \
	pgut/pgut-pthread.c

OBJS = $(SRCS:.c=.o)
DATA = pg_statsrepo.sql pg_statsrepo_alert.sql uninstall_pg_statsrepo.sql
SCRIPTS = archive_pglog.sh
PROGRAM = pg_statsinfod

PG_CPPFLAGS = -I$(libpq_srcdir) -DFRONTEND -DPGUT_MULTI_THREADED -DPGUT_OVERRIDE_ELOG
PG_LIBS = $(libpq) -lpthread

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

# exclude libraries which are not required
LIBS := $(filter -lpgport -lpgcommon, $(LIBS))
