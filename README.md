AgensGraph: Powerful Graph Database
====================================

[![Build Status](https://travis-ci.org/bitnine-oss/agensgraph.svg?branch=master)](https://travis-ci.org/bitnine-oss/agensgraph)

AgensGraph is a new generation multi-model graph database for the modern complex data environment. AgensGraph is a multi-model database, which supports the relational and graph data model at the same time that enables developers to integrate the legacy relational data model and the flexible graph data model in one database. AgensGraph supports ANSI-SQL and openCypher (http://www.opencypher.org). SQL queries and Cypher queries can be integrated into a single query in AgensGraph.

AgensGraph is based on the powerful PostgreSQL RDBMS, and is very robust, fully-featured and ready for enterprise use. AgensGraph is optimized for handling complex connected graph data and provides plenty of powerful database features essential to the enterprise database environment including ACID transactions, multi-version concurrency control, stored procedure, triggers, constraints, sophisticated monitoring and a flexible data model (JSON). Moreover, AgensGraph leverages the rich eco-systems of PostgreSQL and can be extended with many outstanding external modules, like PostGIS.

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

    * macOS (install Xcode):
        ```bash
        $ xcode-select --install
        ```


3. Configure the source tree in /path/to/agensgraph:
    ```sh
    $ ./configure --prefix=$(pwd)
    ```
    >By default, `make install` will install all the files in `/usr/local/pgsql/bin`, `/usr/local/pgsql/lib` etc.  You want to specify an installation prefix to the current library.
    > If `configure` doesn't find any header with an error message, you can use `--with-includes=/path/to/headers` option.

4. Build & install AgensGraph:
    * Build and install AgensGraph engine
        ```sh
        $ make install
        ```

    * Add the install path to the `PATH` environment variable to allow the modules that need `pg_config` to get necessary installation information. If you installed AgensGraph at the same location as the source directory you pulled from GitHub, then you can use the `ag-env.sh` script, which sets `PATH` and `LD_LIBRARY_PATH` using the current directory. Run the following command to use the script:
        ```sh
        $ . ag-env.sh
        ```
        OR, if you installed AgensGraph elsewhere, you can also do so by editing your `/.bashrc` file (`/.bash_profile` on macOS) with the following command:
        ```sh
        $ echo "export PATH=/path/to/agensgraph/bin:\$PATH" >> ~/.bashrc
        $ echo "export LD_LIBRARY_PATH=/path/to/agensgraph/lib:\$LD_LIBRARY_PATH" >> ~/.bashrc
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
