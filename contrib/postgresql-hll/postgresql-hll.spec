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

# The following lines are from the postgresql90 spec file, postgresql-9.0.spec
%define shortversion 93
%define majorversion 9.3
%define	pgbaseinstdir	/usr/pgsql-%{majorversion}

Summary: Aggregate Knowledge HyperLogLog PostgreSQL extension.
Name: postgresql%{shortversion}-hll
Version: 2.10.0
Release: 0
License: Apache License, Version 2.0
URL: https://github.com/aggregateknowledge/postgresql-hll
Vendor: Aggregate Knowledge, Inc.
Group: System Environment/Base
Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires: postgresql%{shortversion}-devel

%description
Aggregate Knowledge HyperLogLog PostgreSQL extension.

%prep
%setup -q -n %{name}

%build

make

%install
rm -rf $RPM_BUILD_ROOT

# Make a new build root.
mkdir -p $RPM_BUILD_ROOT

mkdir -p $RPM_BUILD_ROOT%{pgbaseinstdir}/share/extension
install -m644 hll.control $RPM_BUILD_ROOT%{pgbaseinstdir}/share/extension
install -m644 hll--2.10.0.sql $RPM_BUILD_ROOT%{pgbaseinstdir}/share/extension

mkdir -p $RPM_BUILD_ROOT%{pgbaseinstdir}/lib
install -m755 hll.so $RPM_BUILD_ROOT%{pgbaseinstdir}/lib

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%doc README.markdown

%dir %{pgbaseinstdir}/share/extension
%{pgbaseinstdir}/share/extension/hll.control
%{pgbaseinstdir}/share/extension/hll--2.10.0.sql

%{pgbaseinstdir}/lib/hll.so

%changelog
* Fri Jan 10 2014 Timon Karnezos <timon.karnezos@gmail.com> - 2.10.0-0
- added binary IO type for hll
* Mon Dec 16 2013 Timon Karnezos <timon.karnezos@gmail.com> - 2.9.0-0
- bitstream_pack fixed to write one byte at a time to avoid writing to unallocated memory
* Tue Jul 16 2013 Timon Karnezos <timon.karnezos@gmail.com> - 2.8.0-0
- hll_add_agg now returns hll_empty on input of an empty set
* Wed Jun 12 2013 Timon Karnezos <timon.karnezos@gmail.com> - 2.7.1-0
- Build fixes for OS X and Debian.
- Documentation fixes.
- Small changes to test format to improve stability across psql versions.
* Tue Dec 11 2012 Ken Sedgwick <ken@bonsai.com> - 2.7-0
- Initial version.
