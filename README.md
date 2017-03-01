AgensGraph: Powerful Graph Database
====================================

[![Build Status](https://travis-ci.org/bitnine-oss/agensgraph.svg?branch=master)](https://travis-ci.org/bitnine-oss/agensgraph)

AgensGraph is a new generation multi-model graph database for the modern complex data environment. AgensGraph is a unique multi-model database, which supports relational and graph data model at the same time. It enables developers to integrate the legacy relational data model and the nobel graph data model in one database. SQL query and Cypher query can be integrated into a single query in AgensGraph. AgensGraph is based on powerful PostgreSQL RDBMS, so it is very robust, fully-featured and ready to enterprise use. It is optimzied for handling complex connected graph data but at the same time, it provides a plenty of powerful database features essential to the enterprise database environment, like ACID transaction, multi version concurrency control, stored procedure, trigger, constraint, sophistrated monitoring and flexible data model (JSON). Moreover, AgensGraph can leverage the rich eco-systems of PostgreSQL and can be extended with many outstanding external modules, like PostGIS.

Builing from Source
-------------------
1. Getting the source
    ```sh
    git clone git@github.com:bitnine-oss/agens-graph.git
    ```

2. Install libraries
    * CENTOS
        ```sh
        yum install gcc glibc glib-common readline readline-devel zlib zlib-devel
        ```
    * Fedora
        ```sh
        dnf install gcc glibc bison flex readline readline-devel zlib zlib-devel
        ```
    * Ubuntu
        ```sh
        sudo apt-get install build-essential libreadline-dev zlib1g-dev flex bison
        ```
    * Mac OS X (if you didn't install xcode yet)
        ```bash
        xcode-select --install
        ```

3. Configure the source tree
    ```sh
    ./configure
    ```
    > If you want to install to a specific folder, you can use `--prefix=/path/to/install` option.

    > If `configure` doesn't find any header with an error message, you can use `--with-includes=/path/to/headers` option.

4. Building & install
    * Build and install AgensGraph engine
        ```sh
        make install
        ```
    * Build and install other contrib and external modules
        ```sh
        make install-world
        ```
        Before compiling contrib modules, it is highly recommended to add the install path to `PATH` environment variable because some of them use `pg_config` to get the installation information.
        > If `make install-world` failed with an error message complaining about `pgxs.mk`, then add the install path to `PATH` environment variable and rerun the command.

        > If you install AgensGraph at the same location with the sources, then you can source `ag-env.sh` script, which sets `PATH` and `LD_LIBRARY_PATH` using the current directory. (Run `. ag-env.sh` to source the script.)

Documentation
-------------
* [Short Guide](http://bitnine.net/support/documents_backup/quick-start-guide-html)

License
-------
* [Apache License 2.0](http://www.apache.org/license/LICENSE-2.0.html)
