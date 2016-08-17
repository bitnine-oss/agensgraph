#
# pg_statsinfo: Makefile
#
#    Copyright (c) 2009-2016, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#
SRCS = \
	pg_statsinfo.c \
	snapshot.c \
	report.c \
	control.c \
	pgut/pgut.c \
	pgut/pgut-fe.c \
	pgut/pgut-list.c

OBJS = $(SRCS:.c=.o)
PROGRAM = pg_statsinfo

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq)

ifndef USE_PGXS
top_builddir = ../../..
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
