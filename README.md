AgensGraph: Powerful Graph Database
====================================

[![Build Status](https://travis-ci.org/bitnine-oss/agensgraph.svg?branch=master)](https://travis-ci.org/bitnine-oss/agensgraph)

AgensGraph is a new generation multi-model graph database for the modern complex data environment. AgensGraph is a unique multi-model database, which supports relational and graph data model at the same time. It enables developers to integrate the legacy relational data model and the nobel graph data model in one database. SQL query and Cypher query can be integrated into a single query in AgensGraph. AgensGraph is based on powerful PostgreSQL RDBMS, so it is very robust, fully-featured and ready to enterprise use. It is optimzied for handling complex connected graph data but at the same time, it provides a plenty of powerful database features essential to the enterprise database environment, like ACID transaction, multi version concurrency control, stored procedure, trigger, constraint, sophistrated monitoring and flexible data model (JSON). Moreover, AgensGraph can leverage the rich eco-systems of PostgreSQL and can be extended with many outstanding external modules, like PostGIS.

Building from the Source Code
-------------------
1. Clone AgensGraph onto your local machine
    ```sh
    $ git clone git@github.com:bitnine-oss/agensgraph.git
    ```

2. Install the necessary libraries and dependencies:
    * CENTOS:
        ```sh
        $ yum install gcc glibc glib-common readline readline-devel zlib zlib-devel
        ```
    * Fedora:
        ```sh
        $ dnf install gcc glibc bison flex readline readline-devel zlib zlib-devel
        ```
    * Ubuntu:
        ```sh
        $ sudo apt-get install build-essential libreadline-dev zlib1g-dev flex bison
        ```
    * Mac OS X (install Xcode):
        ```bash
        $ xcode-select --install
        ```

3. Configure the source tree in /path/to/agensgraph:
    ```sh
    $ ./configure
    ```
    > If you want to install to a specific folder, you can use `--prefix=/path/to/install` option.

    > If `configure` doesn't find any header with an error message, you can use `--with-includes=/path/to/headers` option.

4. Build & install AgensGraph:
    * Build and install AgensGraph engine
        ```sh
        $ make install
        ```
        
    * Add the install path to the `PATH` environment variable to allow the modules that need `pg_config` to get necessary installation information. If you installed AgensGraph at the same location as the source directory you pulled from GitHub, then you can use the `ag-env.sh` script, which sets `PATH` and `LD_LIBRARY_PATH` using the current directory. (Run `. ag-env.sh` to use the script.):
        ```sh
        $ ./sh ag-env.sh
        ```
        OR, if you installed AgensGraph elsewhere, you can also do so by editing your `/.bashrc` file with the following command:
        ```sh
        $ echo "export PATH=/path/to/agensgraph/bin:\$PATH" >> ~/.bashrc
        ```
    * Build and install other contrib and external modules:
        ```sh
        $ make install-world
        ```
        
    * OPTIONAL: Set the `AGDATA` environment variable to easily configure AgensGraph settings when necessary:
        ```sh
        $ echo "export AGDATA=/path/to/agensgraph/data" >> ~/.bashrc 
        ```
        
Documentation
-------------
* [Short Guide](http://bitnine.net/support/documents_backup/quick-start-guide-html)

License
-------
* [Apache License 2.0](http://www.apache.org/license/LICENSE-2.0.html)
