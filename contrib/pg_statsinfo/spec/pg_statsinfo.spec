# SPEC file for pg_statsinfo
# Copyright (c) 2009-2018, NIPPON TELEGRAPH AND TELEPHONE CORPORATION

# Original declaration for pg_statsinfo rpmbuild #

%define _pgdir   /usr/pgsql-10
%define _bindir  %{_pgdir}/bin
%define _libdir  %{_pgdir}/lib
%define _datadir %{_pgdir}/share

## Set general information for pg_statsinfo.
Name:       pg_statsinfo
Version:    10.0
Release:    1%{?dist}
Summary:    Performance monitoring tool for PostgreSQL
Group:      Applications/Databases
License:    BSD
URL:        http://sourceforge.net/projects/pgstatsinfo/
Source0:    %{name}-%{version}.tar.gz
BuildRoot:  %{_tmppath}/%{name}-%{version}-%{release}-%(%{__id_u} -n)

## We use postgresql-devel package
BuildRequires:  postgresql10-devel

%description
pg_statsinfo monitors an instance of PostgreSQL server and gather
the statistics and activities of the server as snapshots.

## pre work for build pg_statsinfo
%prep
%setup -q -n %{name}-%{version}

## Set variables for build environment
%build
USE_PGXS=1 make %{?_smp_mflags}

## Set variables for install
%install
rm -rf %{buildroot}

USE_PGXS=1 make DESTDIR=%{buildroot}

## Install each modules
#  Set install location path
install -d %{buildroot}%{_libdir}
install -d %{buildroot}%{_bindir}
install -d %{buildroot}%{_datadir}/contrib

# Install pg_statsinfo package files
install -m 755 agent/bin/pg_statsinfod				%{buildroot}%{_bindir}/pg_statsinfod
install -m 755 reporter/pg_statsinfo				%{buildroot}%{_bindir}/pg_statsinfo
install -m 755 agent/bin/archive_pglog.sh			%{buildroot}%{_bindir}/archive_pglog.sh
install -m 644 agent/bin/pg_statsrepo.sql			%{buildroot}%{_datadir}/contrib/pg_statsrepo.sql
install -m 644 agent/bin/pg_statsrepo_alert.sql		%{buildroot}%{_datadir}/contrib/pg_statsrepo_alert.sql
install -m 644 agent/bin/uninstall_pg_statsrepo.sql	%{buildroot}%{_datadir}/contrib/uninstall_pg_statsrepo.sql
install -m 755 agent/lib/pg_statsinfo.so			%{buildroot}%{_libdir}/pg_statsinfo.so
install -m 644 agent/lib/pg_statsinfo.sql			%{buildroot}%{_datadir}/contrib/pg_statsinfo.sql
install -m 644 agent/lib/uninstall_pg_statsinfo.sql	%{buildroot}%{_datadir}/contrib/uninstall_pg_statsinfo.sql

%clean
rm -rf %{buildroot}

## Set files for this packages
%files
%defattr(-,root,root)
%{_bindir}/pg_statsinfo
%{_bindir}/pg_statsinfod
%{_bindir}/archive_pglog.sh
%{_libdir}/pg_statsinfo.so
%{_datadir}/contrib/pg_statsrepo.sql
%{_datadir}/contrib/pg_statsrepo_alert.sql
%{_datadir}/contrib/uninstall_pg_statsrepo.sql
%{_datadir}/contrib/pg_statsinfo.sql
%{_datadir}/contrib/uninstall_pg_statsinfo.sql
%doc doc/pg_statsinfo-ja.html
%doc doc/pg_statsinfo.html
%doc doc/image/

## Script to run just before installing the package
%pre
# Check if we can safely upgrade.
# An upgrade is only safe if it's from one of our RPMs in the same version family.
installed=$(rpm -q --whatprovides pg_statsinfo 2> /dev/null)
if [ ${?} -eq 0 -a -n "${installed}" ] ; then
	old_version=$(rpm -q --queryformat='%{VERSION}' "${installed}" 2>&1)
	new_version='%{version}'

	new_family=$(echo ${new_version} | cut -d '.' -f 1)
	old_family=$(echo ${old_version} | cut -d '.' -f 1)

	[ -z "${old_family}" ] && old_family="<unrecognized version ${old_version}>"
	[ -z "${new_family}" ] && new_family="<bad package specification: version ${new_version}>"

	if [ "${old_family}" != "${new_family}" ] ; then
		cat << EOF >&2
******************************************************************
A pg_statsinfo package ($installed) is already installed.
Could not upgrade pg_statsinfo "${old_version}" to "${new_version}".
A manual upgrade is required.

 - Remove 'pg_statsinfo' from shared_preload_libraries and
   all of pg_statsinfo.* parameters in postgresql.conf.
 - Restart the monitored database.
 - Uninstall statsinfo schema from monitored database.
 - Uninstall statsrepo schema from repository database.
   Snapshot is removed by dropping the statsrepo schema.
   Therefore, please backup the repository database as necessary.
 - Remove the existing pg_statsinfo package.
 - Install the new pg_statsinfo package.
 - Restore the parameters of postgresql.conf which was removed first.
 - Restart the monitored database.

This is a brief description of the upgrade process.
Important details can be found in the pg_statsinfo manual.
******************************************************************
EOF
		exit 1
	fi
fi

# History of pg_statsinfo-v10 RPM.
%changelog
* Thu Jan  25 2018 - NTT OSS Center 10.0-1
- pg_statsinfo 10.0 released
