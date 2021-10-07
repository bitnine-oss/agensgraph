#!/bin/sh
#
# Script for building Agens Graph distribution 
#
# This script runs configure with default configure options 
# You can add more options like the install path as follows;
#
# ./config.sh [options]

./configure --with-perl --with-python --with-tcl --with-gssapi --with-openssl --with-pam --with-ldap --with-libxml --with-libxslt $@
