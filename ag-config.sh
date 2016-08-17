#!/usr/bin/env sh
#
# This script configures Agens Graph with default options.
# You can add more options as follows;
#
# ./ag-config.sh debug --prefix="$(pwd)"

print_usage() {
	cat <<EOF
Usage: ag-config.sh [-h | COMMAND] [OPTION]...

where COMMAND is one of:
  dist      configure for distribution (default)
  debug     configure for debugging

OPTION's are the same as of \`configure'
EOF
}

case "$1" in
	-h)
		print_usage
		exit
		;;
	dist)
		shift
		;;
	debug)
		opt=(--enable-debug --enable-cassert CFLAGS="-ggdb -Og -fno-omit-frame-pointer")
		shift
		;;
esac

./configure --with-tcl --with-perl --with-python --with-gssapi --with-pam --with-ldap --with-openssl --with-libxml --with-libxslt "$@" "${opt[@]}"
