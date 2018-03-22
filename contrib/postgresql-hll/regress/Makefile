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

# For each file name "foo":
#
# foo.sql				- test script
# foo.ref				- expected output
# ../testdata/foo.csv	- (optional data file for \copy from pstdin)
# foo.out				- actual output

PSQL      = psql
TEST_DB   = hll_regress

SQL       = $(wildcard *.sql)
OUT      := $(SQL:%.sql=%.out)

PSQLOPTS  = -X --echo-all -P null=NULL       # Print NULL values explicitly.
PGOPTIONS = --client-min-messages=warning    # Output WARNINGS

all:	$(OUT)
	@find . -maxdepth 1 -name '*.diff' -print -quit > failures
	@if test -s failures; then \
		echo ERROR: `ls -1 *.diff | wc -l` / `ls -1 *.sql | wc -l` tests failed; \
		echo; \
		cat *.diff; \
		exit 1; \
	else \
		rm failures; \
		echo `ls -1 *.out | wc -l` / `ls -1 *.sql | wc -l` tests passed; \
	fi

clean:
	rm -f binary.dat *.out *.diff

# If a matching testdata file exists use it as standard input.
# Otherwise the test doesn't need data on stdin.
#
%.out:	%.sql %.ref
	@echo -n $*
	@if test -f ../testdata/$*.csv; then \
	  PGOPTIONS=$(PGOPTIONS) $(PSQL) $(PSQLOPTS) $(TEST_DB) -f $*.sql < ../testdata/$*.csv > $*.out 2>&1; \
	else \
	  PGOPTIONS=$(PGOPTIONS) $(PSQL) $(PSQLOPTS) $(TEST_DB) -f $*.sql > $*.out 2>&1; \
	fi
	@diff -u -I "^COPY.*" $*.ref $*.out >> $*.diff || status=1
	@if test -s $*.diff; then \
		echo " .. FAIL"; \
	else \
		echo " .. PASS"; \
		rm -f $*.diff; \
	fi

