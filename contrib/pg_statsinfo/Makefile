#
# pg_statsinfo: Makefile
#
#    Copyright (c) 2009-2018, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#
SUBDIRS = agent reporter
REGTEST = \
	function-snapshot \
	function-snapshot_replication \
	function-logger \
	function-logstore \
	function-alert \
	function-maintenance \
	function-report \
	function-command_option

ifndef USE_PGXS
top_builddir = ../..
makefile_global = $(top_builddir)/src/Makefile.global
ifeq "$(wildcard $(makefile_global))" ""
USE_PGXS = 1	# use pgxs if not in contrib directory
endif
endif

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = pg_statsinfo
include $(makefile_global)
include $(top_srcdir)/contrib/contrib-global.mk
endif

all install installdirs uninstall distprep clean distclean maintainer-clean:
	@for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@ || exit; \
	done

installcheck:
	( cd test && ./regress.sh $(REGTEST))
