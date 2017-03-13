HADOOP_FDW
==========

Foreign Data Wrapper (FDW) that facilitates access to Hadoop from within PostgreSQL 9.5.

## Prepare

In addition to normal PostgreSQL FDW pre-reqs, the primary specific
requirements for this FDW are a JDK (we test with JDK 8) and a set of
Hive client JAR files for the Hadoop distribution you are connecting
with.

## Install

This FDW is included in the BigSQL by PostgreSQL distribution.  All you
have to do is follow the usage instructions below.

## Building from Source

First, download the source code under the contrib subdirectory of the
PostgreSQL source tree and then build and install the FDW as below:

1) Create a link to your JVM in the PostgreSQL lib folder

```
cd PathToFile/lib # cd to the PostgreSQL lib folder
ln -s PathToFile/libjvm.so libjvm.so
```

2) Build the FDW source

```
cd hadoop_fdw
USE_PGXS=1 make
USE_PGXS=1 make install # with sudo if necessary
```

## To execute the FDW

1) Set the environment variables PGHOME,HIVE_HOME,HADOOP_HOME & HADOOP_JDBC_CLASSPATH before starting up PG.These      environment variables are read at the JVM initialisation time.

    PGHOME = Path to the PostgreSQL installation. Considering PostgreSQL is installed under /usr/local/pgsql/ the $PGHOME is /usr/local/pgsql/
    HIVECLIENT_JAR_HOME = The path containing the Hive JDBC client jar files required for the FDW to run successfully.
    HADOOP_JDBC_CLASSPATH = .:$(echo $HIVECLIENT_JAR_HOME/*.jar |  tr ' ' :):/PathToFile/hadoop-core-1.2.1.jar

## Usage

The following parameters can be set on a Hive2 foreign server object:

  * **`host`**: the address or hostname of the Hive2 server, Examples: "localhost" "127.0.0.1" "server1.domain.com".
  * **`port`**: the port number of the Hive2 server.


The following parameters can be set on a Hadoop foreign table object:

  * **`schema_name`**: the name of the schema in which the table exists. Defaults to "default".
  * **`table_name`**: the name of the Hive table to query.  Defaults to the foreign table name used in the relevant CREATE command.

Here is an example:


	-- load EXTENSION first time after install.
	CREATE EXTENSION hadoop_fdw;

        -- create server object
	CREATE SERVER hadoop_serv FOREIGN DATA WRAPPER hadoop_fdw
		OPTIONS(host 'localhost', port '10000');

	-- Create a user mapping for the server.
	CREATE USER MAPPING FOR public SERVER hadoop_serv OPTIONS(username 'test', password 'test');

	-- Create a foreign table on the server.
	CREATE FOREIGN TABLE test (id int) SERVER hadoop_serv OPTIONS (schema 'exmaple',table 'oorder');

	-- Query the foreign table.
	SELECT * FROM test limit 5;
