#-------------------------------------------------------------------------
#
# Makefile for src/bin/ag_ctl
#
# Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/ag_ctl/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "ag_ctl - starts/stops/restarts the PostgreSQL server"
PGAPPICON=win32

subdir = src/bin/ag_ctl
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# On Windows, we need to link with libpq, just for use of pqexpbuffer;
# but let's not pull that in on platforms where we don't need it.
ifeq ($(PORTNAME), win32)
override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)
LDFLAGS_INTERNAL += $(libpq_pgport)
SUBMAKE_LIBPQ := submake-libpq
endif

OBJS=	../pg_ctl/pg_ctl.o $(WIN32RES)

all: ag_ctl

ag_ctl: $(OBJS) | submake-libpgport $(SUBMAKE_LIBPQ)
	$(CC) $(CFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) ag_ctl$(X) '$(DESTDIR)$(bindir)/ag_ctl$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/ag_ctl$(X)'

clean distclean maintainer-clean:
	rm -f ag_ctl$(X) $(OBJS)
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
