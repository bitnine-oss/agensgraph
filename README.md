# AgensGraph: Powerful Multi-Model Graph Database
![Build Status](https://github.com/skaiworldwide-oss/agensgraph/actions/workflows/regression.yml/badge.svg)
&nbsp;
<a href="https://github.com/skaiworldwide-oss/agensgraph/releases">
<img src="https://img.shields.io/badge/Release-v2.16.0-FFA500?labelColor=gray&style=flat&link=https://github.com/skaiworldwide-oss/agensgraph/releases"/>
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
- **Query Languages**: Supports ANSI-SQL and openCypher ([openCypher](http://www.opencypher.org)) and partially ISO/GQL.
- **Seamless Integration**: Integrate SQL and Cypher queries within a single query.
- **Enterprise-Ready**: ACID transactions, multi-version concurrency control, stored procedures, triggers, constraints, sophisticated monitoring, and flexible data models (JSON).
- **Extensible**: Leverages PostgreSQL's rich ecosystem, including modules like PostGIS.
## Quick Start with Docker

1. **Pull the AgensGraph Docker image**

    ```bash
    docker pull skaiworldwide/agensgraph
    ```
    **Note**: By default, this pulls the `latest` tag

2. **Create and run the AgensGraph container**

    ```bash
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
    ```bash
    docker exec -it agensgraph agens -d agens -U postgres
    ```

**More Information**

See more information on the [Docker Hub](https://hub.docker.com/r/skaiworldwide/agensgraph) page.

## Source Code Compilation

To build, install and setup AgensGraph for source, follow instructions in [Installation Guide](INSTALL.md)


## Performance Tuning for Graph Workloads

PostgreSQL's defaults are tuned for small relational queries. Graph analytics — neighbourhood aggregation, top-degree, multi-hop traversals — run large hash/sort operations that spill to disk at the default `work_mem`, and benefit from parallelism. The settings below are a good starting point for an analytics or mixed graph workload on a multi-core server.

Apply them with `ALTER SYSTEM SET ...;` followed by `SELECT pg_reload_conf();` (or by editing `postgresql.conf`), or `SET` them per session.

```conf
# Memory: avoid disk spills in graph aggregation/sort
work_mem = 256MB                # per sort/hash node; raise for heavy GROUP BY / DISTINCT
maintenance_work_mem = 1GB      # faster index builds and VACUUM on large edge labels

# Planner: trust indexes and account for the cache
effective_cache_size = <~70% of RAM>
random_page_cost = 1.1          # for SSDs, where index scans are nearly as cheap as sequential

# Parallelism: speeds up full-edge-scan analytics
max_parallel_workers_per_gather = 4
max_parallel_workers = <number of CPU cores>
max_worker_processes = <number of CPU cores>
```

Notes:
- `work_mem` is allocated **per sort/hash node, per connection**, so a large value combined with many concurrent clients can over-commit memory. Size it to roughly `RAM / expected_concurrency`, or raise it only per session for heavy analytic queries.
- Parallelism helps large full-scan analytics but adds worker-startup overhead to small point or single-hop reads; lower `max_parallel_workers_per_gather` if your workload is mostly short, OLTP-style queries.
- After bulk-loading a graph, run `ANALYZE` so the planner has accurate statistics for traversal joins.

## AgensGraph AI Integration
[AgensGraph-AI Repository](https://github.com/skaiworldwide-oss/agensgraph-ai) provide collection of tools, integrations, and starter templates for building AI-powered applications that work with AgensGraph.

## AgensGraph Drivers
AgensGraph supports various drivers for seamless connection and interaction with the database. Below are the supported drivers:
| Driver      | Description                                                                                       |
|-------------|---------------------------------------------------------------------------------------------------|
| **JDBC**    | [JDBC Driver](https://github.com/skaiworldwide-oss/agensgraph-jdbc) <br> Enables Java applications to interact with AgensGraph. |
| **Python**  | [Python Driver](https://github.com/skaiworldwide-oss/agensgraph-python) <br> Facilitates interaction between Python applications and AgensGraph. |
| **Node.js** | [Node.js Driver](https://github.com/skaiworldwide-oss/agensgraph-nodejs) <br> Allows Node.js applications to interface with AgensGraph. |
| **Go**      | [Go Driver](https://github.com/skaiworldwide-oss/agensgraph-golang) <br> Provides connectivity for Go applications to AgensGraph. |


## Documentation
[AgensGraph Manual](http://tech.skaiworldwide.com/docs/en/agensgraph/latest/) is available to help you get started with AgensGraph and make the most of its features.

## AGViewer
AGViewer is a web-based user interface that provides visualization of graph data stored in an AgensGraph database. It allows users to easily interact with and visualize their graph data, making it easier to understand and analyze complex relationships within the database.
- **Web-Based Interface**: Accessible through any web browser.
- **Graph Visualization**: Provides interactive visualization tools for graph data.
- **User-Friendly**: Intuitive interface designed for ease of use.
- **Real-Time Interaction**: Allows for real-time data updates and interaction with graph data.
<img src="https://github.com/skaiworldwide-oss/agensgraph/blob/main/images/g_result_1.png" alt="AGViewer Screenshot" width="400" />

For more information and to get started with AGViewer, visit the [AGViewer GitHub repository](https://github.com/skaiworldwide-oss/AGViewer).

## License
- [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0)

