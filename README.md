AgensGraph: Powerful Graph Database
====================================

[![Build Status](https://travis-ci.org/bitnine-oss/agens-graph.svg?branch=master)](https://travis-ci.org/bitnine-oss/agens-graph)

AgensGraph is a new generation multi-model graph database for the modern complex data environment. AgensGraph is a unique multi-model database, which supports relational and graph data model at the same time. It enables developers to integrate the legacy relational data model and the nobel graph data model in one database. SQL query and Cypher query can be integrated into a single query in AgensGraph. AgensGraph is based on powerful PostgreSQL RDBMS, so it is very robust, fully-featured and ready to enterprise use. It is optimzied for handling complex connected graph data but at the same time, it provides a plenty of powerful database features essential to the enterprise database environment, like ACID transaction, multi version concurrency control, stored procedure, trigger, constraint, sophistrated monitoring and flexible data model (JSON). Moreover, AgensGraph can leverage the rich eco-systems of PostgreSQL and can be extended with many outstanding external modules, like PostGIS.

Builing from Source
-------------------
1. Getting the source
    ```bash
    git clone git@github.com:bitnine-oss/agens-graph.git /path/
    ```

2. Install libraries
    * CENTOS
        ```bash
        yum install gcc glibc glib-common readline readline-devel zlib zlib-devel
        ```
    * Fedora
        ```bash
        dnf install gcc glibc bison flex readline readline-devel zlib zlib-devel
        ```
    * Ubuntu
        ```bash
        sudo apt-get install build-essential libreadline-dev zlib1g-dev flex bison
        ```
    * Mac OS X(If you didn't install xcode.)
        ```bash
        xcode-select --install
        ```

3. Configure the source tree
    ```bash
	./configure
    ```
    >If you want to install to a specific folder, you can use ``--prefix=[INSTALL_PATH]`` option.
    >If ``configure`` doesn't find any header with an error message, you can use '--with-includes=/path/' option.

4. Building & install
    ```bash
    make world
    make install-world
    ```

Documentation
-------------
* [Short Guide](http://bitnine.net/support/documents_backup/quick-start-guide-html)

License
-------

* [Apache Lincense 2.0](http://www.apache.org/license/LICENSE-2.0.html)
