# AgensGraph: Powerful Multi-Model Graph Database
![Build Status](https://github.com/skaiworldwide-oss/agensgraph/actions/workflows/regression.yml/badge.svg)
&nbsp;
<a href="https://github.com/skaiworldwide-oss/agensgraph/releases">
<img src="https://img.shields.io/badge/Release-v2.15.0-FFA500?labelColor=gray&style=flat&link=https://github.com/skaiworldwide-oss/agensgraph/releases"/>
</a>
&nbsp;
<a href="https://github.com/skaiworldwide-oss/agensgraph/issues">
  <img src="https://img.shields.io/github/issues/skaiworldwide-oss/agensgraph"/>
</a>
&nbsp;
<a href="https://github.com/skaiworldwide-oss/agensgraph/network/members">
 <img src="https://img.shields.io/github/forks/skaiworldwide-oss/agensgraph"/>
</a>
&nbsp;
<a href="https://github.com/skaiworldwide-oss/agensgraph/stargazers">
 <img src="https://img.shields.io/github/stars/skaiworldwide-oss/agensgraph"/>
</a>
<br>

AgensGraph is a cutting-edge multi-model graph database designed for modern complex data environments. By supporting both relational and graph data models simultaneously, AgensGraph allows developers to seamlessly integrate legacy relational data with the flexible graph data model within a single database. AgensGraph is built on the robust PostgreSQL RDBMS, providing a highly reliable, fully-featured platform ready for enterprise use.

## Key Features
- **Multi-Model Support**: Combines relational and graph data models.
- **Query Languages**: Supports ANSI-SQL and openCypher ([openCypher](http://www.opencypher.org)).
- **Seamless Integration**: Integrate SQL and Cypher queries within a single query.
- **Enterprise-Ready**: ACID transactions, multi-version concurrency control, stored procedures, triggers, constraints, sophisticated monitoring, and flexible data models (JSON).
- **Extensible**: Leverages PostgreSQL's rich ecosystem, including modules like PostGIS.

## Building from the Source
Follow these steps to build AgensGraph from the source code:
1. **Clone the Repository**
    ```sh
    git clone https://github.com/skaiworldwide-oss/agensgraph.git
    ```

2. **Install Dependencies**
    - **Rocky Linux**:
        ```sh
        yum install gcc glibc readline readline-devel zlib zlib-devel perl
        ```

    - **Fedora**:
        ```sh
        dnf install gcc glibc bison flex readline readline-devel zlib zlib-devel
        ```

    - **RHEL**:
        ```sh
        yum install gcc glibc glib-common readline readline-devel zlib zlib-devel flex bison
        ```

    - **Ubuntu**:
        ```sh
        sudo apt-get install build-essential libreadline-dev zlib1g-dev flex bison
        ```

    - **macOS** (install Xcode):
        ```bash
        xcode-select --install
        ```

3.  **Configure the Source Tree**
    ```sh
    ./configure --prefix=$(pwd)
    ```
    > By default, `make install` installs files in `/usr/local/pgsql/bin`, `/usr/local/pgsql/lib`, etc. Specify an installation prefix to the current directory. If `configure` encounters missing headers, use `--with-includes=/path/to/headers`.

4. **Build & install AgensGraph**:
    ```sh
    make install
    ```

5. **Set Up Environment Variables**
    - Add the install path to the `PATH` environment variable:
        ```sh
        . ag-env.sh
        ```
      OR, edit your `/.bashrc` file (`/.bash_profile` on macOS):
        ```sh
        echo "export PATH=/path/to/agensgraph/bin:\$PATH" >> ~/.bashrc
        echo "export LD_LIBRARY_PATH=/path/to/agensgraph/lib:\$LD_LIBRARY_PATH" >> ~/.bashrc
        ```
6. **Optional: Build and Install with Additional Modules**
    ```sh
    make install-world
    ```
7. **Optional: Set `AGDATA` Environment Variable**
    ```sh
    echo "export AGDATA=/path/to/agensgraph/data" >> ~/.bashrc
    ```
## Quick Start with Docker

1. **Pull the AgensGraph Docker image**

    ```bash
    docker pull skaiworldwide/agensgraph
    ```
    **Note**: By default, this pulls the `latest` tag

2. **Create and run the AgensGraph container**

    ```
    docker run \
        --name agensgraph \
        -p 5455:5432 \
        -e POSTGRES_USER=postgres \
        -e POSTGRES_PASSWORD=agens \
        -e POSTGRES_DB=agens \
        -d \
        skaiworldwide/agensgraph
    ```

3. **Connect to AgensGraph client**
    ```
    docker exec -it agensgraph agens -d agens -U postgres
    ```

**More Information**

See more information on the [Docker Hub](https://hub.docker.com/r/skaiworldwide/agensgraph) page.

## AgensGraph Drivers
AgensGraph supports various drivers for seamless connection and interaction with the database. Below are the supported drivers:
| Driver      | Description                                                                                       |
|-------------|---------------------------------------------------------------------------------------------------|
| **JDBC**    | [JDBC Driver](https://github.com/skaiworldwide-oss/agensgraph-jdbc) <br> Enables Java applications to interact with AgensGraph. |
| **Python**  | [Python Driver](https://github.com/skaiworldwide-oss/agensgraph-python) <br> Facilitates interaction between Python applications and AgensGraph. |
| **Node.js** | [Node.js Driver](https://github.com/skaiworldwide-oss/agensgraph-nodejs) <br> Allows Node.js applications to interface with AgensGraph. |
| **Go**      | [Go Driver](https://github.com/skaiworldwide-oss/agensgraph-golang) <br> Provides connectivity for Go applications to AgensGraph. |


## Documentation
Comprehensive documentation is available to help you get started with AgensGraph and make the most of its features.
- **Quick Start Guide**: Learn how to quickly set up and start using AgensGraph.
  - [Quick Start Guide](https://www.skaiworldwide.com/_next/static/pdf/agensgraph%20manual%20pdf_quick%20guide_(EN).pdf)

## AgensGraphViewer
AgensGraphViewer is a web-based user interface that provides visualization of graph data stored in an AgensGraph database. It allows users to easily interact with and visualize their graph data, making it easier to understand and analyze complex relationships within the database.
- **Web-Based Interface**: Accessible through any web browser.
- **Graph Visualization**: Provides interactive visualization tools for graph data.
- **User-Friendly**: Intuitive interface designed for ease of use.
- **Real-Time Interaction**: Allows for real-time data updates and interaction with graph data.
<img src="https://github.com/skaiworldwide-oss/agensgraph/blob/main/images/g_result_1.png" alt="AgensGraphViewer Screenshot" width="400" />

For more information and to get started with AgensGraphViewer, visit the [AgensGraphViewer GitHub repository](https://github.com/skaiworldwide-oss/AgensGraphViewer).

## License
- [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0)
