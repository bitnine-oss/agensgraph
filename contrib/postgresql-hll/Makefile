# Copyright 2013 Aggregate Knowledge, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

MODULE_big = hll
OBJS =		\
			hll.o \
			MurmurHash3.o

EXTENSION = hll
DATA =		\
			hll--2.10.0.sql

EXTRA_CLEAN += -r $(RPM_BUILD_ROOT)

PG_CPPFLAGS += -fPIC
hll.o: override CFLAGS += -std=c99
MurmurHash3.o: override CC = $(CXX)

ifdef DEBUG
COPT		+= -O0
CXXFLAGS	+= -g -O0
endif

SHLIB_LINK	+= -lstdc++

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/postgresql-hll
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
