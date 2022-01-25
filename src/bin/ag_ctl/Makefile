#-------------------------------------------------------------------------
#
# Makefile for src/bin/ag_ctl
#
# Portions Copyright (c) 2017, Bitnine Inc.
# Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/ag_ctl/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "ag_ctl - starts/stops/restarts the AgensGraph server"
PGAPPICON=win32

subdir = src/bin/ag_ctl
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

OBJS=	ag_ctl.o $(WIN32RES)

all: ag_ctl

ag_ctl: $(OBJS) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) $(OBJS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

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
