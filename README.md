Agens Graph: Powerful Graph Database
====================================

[![Build Status](https://travis-ci.org/bitnine-oss/agens-graph.svg?branch=master)](https://travis-ci.org/bitnine-oss/agens-graph)

Agens Graph is a new generation multi-model graph database for the modern complex data environment. Agens Graph is a unique multi-model database, which supports relational and graph data model at the same time. It enables developers to integrate the legacy relational data model and the nobel graph data model in one database. SQL query and Cypher query can be integrated into a single query in Agens Graph. Agens Graph is based on powerful PostgreSQL RDBMS, so it is very robust, fully-featured and ready to enterprise use. It is optimzied for handling complex connected graph data but at the same time, it provides a plenty of powerful database features essential to the enterprise database environment, like ACID transaction, multi version concurrency control, stored procedure, trigger, constraint, sophistrated monitoring and flexible data model (JSON). Moreover, Agens Graph can leverage the rich eco-systems of PostgreSQL and can be extended with many outstanding external modules, like PostGIS. 

Builing from Source
-------------------
1. Getting the source
    ```bash
    git clone git@github.com:bitnine-oss/agens-graph.git /path/
    ```

2. Install libraries
    * CENTOS
        ```bash
        yum install gcc glibc glib-common perl-ExtUtils-Embed readline readline-devel zlib zlib-devel openssl openssl-devel  pam pam-devel libxml2 libxml2-devel libxslt libxslt-devel ldap libldap-devel libpam0g-dev openldap-devel tcl tcl-devel python-devel
        ```
    * Fedora
        ```bash
        dnf install gcc glibc bison flex perl-ExtUtils-Embed readline readline-devel zlib zlib-devel openssl openssl-devel  pam pam-devel libxml2 libxml2-devel libxslt libxslt-devel openldap-devel tcl tcl-devel python-devel
        ```
    * Ubuntu
        ```bash
        sudo apt-get install build-essential libreadline-dev zlib1g-dev flex bison libxml2-dev libxslt-dev libssl-dev openssl libgnutls-openssl27 libcrypto++-dev libldap2-dev libpam0g-dev tcl-dev python-dev
        ```
    * Mac OS X(If you didn't install xcode.)
        ```bash
        xcode-select --install
        ```

3. Configure the source tree
    ```bash
    ag-config.sh
    ```
    >If you want to install to a specific folder, you can use ``--prefix=[INSTALL_PATH]`` option.
    >If ``ag-config.sh`` doesn't find any header with an error message, you can use '--with-includes=/path/' option.

4. Building & install
    ```bash
    make world
    make install-world
    ```

License
-------

* [Apache Lincense 2.0](http://www.apache.org/license/LICENSE-2.0.html)


