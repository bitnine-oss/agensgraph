Agens Graph: Powerful Graph Database
====================================

[![Build Status](https://travis-ci.org/bitnine-oss/agens-graph.svg?branch=master)](https://travis-ci.org/bitnine-oss/agens-graph)

Agens Graph is a new generation multi-model graph database for the modern complex data environment. Agens Graph is a unique multi-model database, which supports relational and graph data model at the same time. It enables developers to integrate the legacy relational data model and the nobel graph data model in one database. SQL query and Cypher query can be integrated into a single query in Agens Graph. Agens Graph is based on powerful PostgreSQL RDBMS, so it is very robust, fully-featured and ready to enterprise use. It is optimzied for handling complex connected graph data but at the same time, it provides a plenty of powerful database features essential to the enterprise database environment, like ACID transaction, multi version concurrency control, stored procedure, trigger, constraint, sophistrated monitoring and flexible data model (JSON). Moreover, Agens Graph can leverage the rich eco-systems of PostgreSQL and can be extended with many outstanding external modules, like PostGIS. 

Builing from Source
-------------------

In Linux, you need to run ``configure``

`` ./configure ``

If you want to install to a specific foler, you can do

`` ./configure --prefix=[INSTALL_PATH] ``

then,
 
`` make ``

If you want to include various contrib modules, you can do

`` make world ``

* Install

`` make install ``


License
-------

* [Apache Lincense 2.0](http://www.apache.org/license/LICENSE-2.0.html)


