# This script sets ./bin to $PATH and ./lib to $LD_LIBRARY_PATH instead of
# existing path elements contain 'postgres'.
# It is handy when you installed Agens Graph in the current directory for
# development purpose.
#
# Usage: . ag-env.sh

export AG_HOME="$(pwd)"

_path=$(echo $PATH | tr ':' '\n' | grep -vE "postgres|agens-graph|^$" | tr '\n' ':' | sed 's/:$//')
export PATH=$AG_HOME/bin:$_path

_path=$(echo $LD_LIBRARY_PATH | tr ':' '\n' | grep -vE "postgres|agens-graph|^$" | tr '\n' ':' | sed 's/:$//')
export LD_LIBRARY_PATH=$AG_HOME/lib:$_path

unset _path
