# Apache Spark: A Unified Engine for Distributed Data Processing

## Key Characteristics of Spark

### Speed

- **Efficient Hardware Utilization**: Leverages multi-core CPUs, large memory, and Unix-based multithreading.
- **DAG Scheduler**: Constructs a Directed Acyclic Graph (DAG) for parallel execution across clusters.
- **Tungsten Engine**: Optimized with whole-stage code generation for fast execution, retaining intermediate results in
  memory.

### Ease of Use

- **Abstractions**: Built on Resilient Distributed Datasets (RDDs), with higher-level APIs like DataFrames and Datasets.
- **Simple Programming Model**: Operations available in familiar languages (Python, Java, Scala, SQL, R).

### Modularity

- Supports multiple workloads and programming languages.
- Includes core libraries:
    - **Spark SQL**: Structured data processing.
    - **Spark MLlib**: Machine learning library.
    - **Spark Structured Streaming**: Real-time data processing.
    - **GraphX**: Graph computation.

### Extensibility

- **Storage-Agnostic**: Reads data from multiple sources (Hadoop, Cassandra, Hive, MongoDB, S3, etc.).
- **Extendable Readers/Writers**: Works with Kafka, Kinesis, Azure Storage, and more.

## Unified Analytics Components

### Spark SQL

- Processes structured data from file formats (CSV, JSON, Parquet, etc.) or RDBMS.
- Supports ANSI SQL:2003 and can integrate SQL queries with Spark APIs.

### Spark MLlib

- High-level APIs for machine learning tasks:
    - Feature extraction, pipelines, model persistence.
    - Includes linear algebra utilities and optimization algorithms.

### Spark Structured Streaming

- Processes streaming data as a continually growing table.
- Built atop Spark SQL engine for fault tolerance and late data semantics.

### GraphX

- Graph-based computations (e.g., social network analysis, PageRank, Connected Components).
- Provides standard graph algorithms for various use cases.

## Spark Architecture

### Spark Driver

- Orchestrates parallel operations:
    - Requests resources from the cluster manager.
    - Transforms Spark operations into a DAG and schedules tasks.

### Spark Session

- Unified entry point to all Spark functionality.
- Creates runtime parameters, defines DataFrames/Datasets, reads data sources, and executes SQL queries.

### Cluster Manager

- Allocates resources for the cluster nodes.
- Supported managers: Standalone, YARN, Mesos, Kubernetes.

### Spark Executor

- Runs tasks on worker nodes.
- Communicates with the driver and processes data partitions.

## Distributed Data and Partitions

- **Data Partitioning**: Physical data is split into chunks (partitions) stored across a cluster.
- **Data Locality**: Executors process partitions close to them to minimize network bandwidth.
- **Efficient Parallelism**: Each executor handles its own data partition for maximum performance.

---

# Spark Concepts

## Key Terminologies

### Application

- A user program built on Spark using its APIs.
- Consists of a **driver program** and **executors** running on a cluster.

### SparkSession

- An object providing the entry point to interact with Spark functionality.
- Enables programming Spark with its APIs.
- In an interactive Spark shell, the driver automatically instantiates the SparkSession.
- In Spark applications, users manually create the SparkSession object.

### Job

- A parallel computation consisting of multiple tasks.
- Triggered by Spark **actions** (e.g., `save()`, `collect()`).

### Stage

- A job is divided into smaller units called **stages**, which depend on each other.
- Represents a segment of operations that can be executed serially or in parallel.

### Task

- The smallest unit of work in Spark, sent to an executor.
- Each task operates on a single data partition.

## Spark Application and Spark Session

- At the core of every Spark application is the **Spark driver program**.
- The driver creates a **SparkSession**, allowing you to perform Spark operations.
- In interactive sessions (e.g., Spark shell), the driver and SparkSession are pre-created.

## Spark Jobs, Stages, and Tasks

### Spark Jobs

- A Spark job is generated in response to an **action**.
- The driver converts jobs into a **DAG (Directed Acyclic Graph)**.
- The DAG represents the execution plan, broken into stages.

### Spark Stages

- Stages are created based on data dependencies:
    - **Parallel stages**: Independent operations executed simultaneously.
    - **Serial stages**: Operations dependent on the output of previous stages.

### Spark Tasks

- Each stage consists of **tasks**.
- Tasks execute in parallel across Spark executors.
- Executors process tasks corresponding to individual data partitions.

## Transformations, Actions, and Lazy Evaluation

### Transformations

- Operations that produce a new DataFrame without altering the original data (e.g., `select()`, `filter()`).
- **Immutable**: The original DataFrame remains unchanged.
- **Lazy Evaluation**: Transformations are recorded as a **lineage** but not executed until an action is triggered.

### Actions

- Operations that trigger the execution of transformations (e.g., `count()`, `collect()`).
- Spark optimizes the execution plan before performing the action.

### Benefits of Lazy Evaluation

- **Query Optimization**: Rearranges and combines transformations for efficient execution.
- **Fault Tolerance**: Lineage allows Spark to recompute lost data by replaying transformations.

## Narrow and Wide Transformations

### Narrow Transformations

- Output partition depends on a single input partition.
- Example: `filter()`, `map()`.
- No data shuffling required, making these operations efficient.

### Wide Transformations

- Output partition depends on multiple input partitions.
- Example: `groupBy()`, `orderBy()`.
- Requires **data shuffling** across the cluster, incurring additional overhead.

---

# **Spark Structured APIs**

## What's Underneath an RDD?

- **Characteristics:**
    - **Dependencies:** Define reproducibility paths in case of data loss or recomputation.
    - **Partitions:** Enable parallelism by splitting data into manageable chunks.
    - **Compute Function:** Operates on partitions to produce results (`Partition => Iterator[T]`).

- **Limitations of RDDs:**
    1. **Opaque Computations:** Compute functions (e.g., joins, filters) are treated as black-box lambda expressions,
       limiting optimization.
    2. **Opaque Data Types:** Spark doesn't recognize specific types or columns in Python RDDs.
    3. **Inefficiency:** Without understanding computations or data types, Spark cannot optimize expressions or leverage
       compression.

## Structuring Spark

- Spark 2.x introduced structured APIs to enhance clarity and performance:
    - **High-level operations:** Filtering, selecting, counting, aggregating, averaging, grouping.
    - **Domain-Specific Language (DSL):** Available as APIs in Java, Python, Scala, R, and SQL.
    - Enables Spark to construct efficient query plans automatically.

## DataFrame API

- **Description:** Distributed, in-memory tables with named columns and schemas. Inspired by pandas DataFrames.
- **Key Features:**
    - **Immutable:** DataFrames maintain a lineage of transformations.
    - **Schema-aware:** Columns have specific data types (e.g., `IntegerType`, `StringType`, etc.).
    - **Structured and Complex Data Types:**
        - `ArrayType`, `MapType`, `StructField`, `TimestampType`, etc.

### Schemas and Creating DataFrames

- **Schema Benefits:**
    1. Avoids Spark inferring data types, saving resources.
    2. Eliminates separate jobs for schema inference in large files.
    3. Detects errors early when data mismatches schema.
- **Defining Schemas:**
    - Programmatically or using Data Definition Language (DDL) strings.

### Columns and Rows

- **Columns:** Named fields representing data types. Operations can be performed using relational or computational
  expressions.
- **Rows:** Generic objects containing one or more columns, accessed using index-based methods.

## Dataset API

- **Unification of DataFrame and Dataset APIs in Spark 2.0.**
- **Typed vs. Untyped Objects:**
    - **DataFrame:** Alias for `Dataset[Row]` (untyped).
    - **Dataset:** Strongly typed JVM objects in Java/Scala. Each Dataset has an untyped view (`DataFrame`).

### Creating Datasets

- **Schema Knowledge:** Essential for creating Datasets.
    - JSON/CSV data allows schema inference, but it is resource-intensive for large datasets.

## DataFrames vs. Datasets

| **Aspect**              | **DataFrame**                                                              | **Dataset**                                                                          |
|-------------------------|----------------------------------------------------------------------------|--------------------------------------------------------------------------------------|
| **Definition**          | A distributed collection of rows, represented as `Dataset[Row]` (untyped). | A strongly typed distributed collection of objects.                                  |
| **Typing**              | Untyped API (`Row` objects).                                               | Strongly typed API with JVM objects.                                                 |
| **Languages Supported** | Available in Python, R, Scala, and Java.                                   | Available only in Scala and Java.                                                    |
| **Ease of Use**         | Simpler to use for most high-level operations.                             | Requires knowledge of object types and schemas.                                      |
| **Performance**         | Optimized for memory and space, using Tungsten.                            | Provides additional compile-time optimizations.                                      |
| **Error Detection**     | Errors detected only at runtime.                                           | Errors detected at compile-time in Scala/Java.                                       |
| **Serialization**       | Uses Spark SQL's optimized binary serialization.                           | Uses Spark SQL's optimized serialization plus Java serialization for custom objects. |
| **Use Cases**           | SQL-like transformations and relational queries.                           | Type-safe operations and transformations.                                            |
| **Flexibility**         | Suitable for dynamic data structures.                                      | Ideal for static schemas with strict typing.                                         |
| **Examples**            | - Operations on structured data like JSON, CSV, Parquet.                   | - Working with domain-specific JVM objects.                                          |

### **When to Use:**

- **DataFrame:**
    - If working in Python or R.
    - For SQL-like queries, dynamic data, and simplicity.
- **Dataset:**
    - When strong typing, compile-time safety, and better debugging are required.
    - Primarily used in Java/Scala for type-safe operations.

### **Key Note:**

In Python and R, only DataFrames are available. In Scala and Java, you can choose based on the requirements for typing
and error handling.

## Spark SQL and Underlying Engine

- **Features:**
    - ANSI SQL:2003-compatible queries on structured data with schemas.
    - Unifies Spark components into a single abstraction: DataFrames/Datasets.
    - Connects with Hive Metastore and external tools via JDBC/ODBC.
    - Reads/writes structured file formats (JSON, CSV, Avro, Parquet, etc.).

### Key Components:

1. **Catalyst Optimizer:**
    - Converts computational queries into execution plans through:
        1. Analysis.
        2. Logical Optimization.
        3. Physical Planning.
        4. Code Generation.
2. **Project Tungsten:**
    - Focuses on memory and CPU efficiency through binary processing and code generation.

---

# SparkSQL & DataFrames: Built-in Data Sources

## SQL Tables and Views

### Tables and Metadata

- **Tables** store data, and their **metadata** (schema, column names, physical location, etc.) is managed in a central
  **metastore**.
- Spark uses the **Apache Hive metastore** by default, located at `/user/hive/warehouse`.
- The default location can be changed by setting `spark.sql.warehouse.dir` to a custom path.

### Managed vs. Unmanaged Tables

- **Managed Tables**: Spark manages both metadata and data. Dropping a managed table deletes both.
- **Unmanaged Tables**: Spark manages only metadata. You manage the data externally. Dropping an unmanaged table deletes
  only the metadata.

### Creating Databases and Tables

- Tables are stored in databases, defaulting to the `default` database.
- You can create a new database by issuing a SQL command in your Spark application or notebook.

### Creating Views

- **Views** are virtual tables that don’t store data.
- **Global Views**: Accessible across all Spark sessions in a cluster.
- **Session-scoped Views**: Visible only to the current session.
- To query a global view, use `global_temp.<view_name>`, as Spark creates them in a special `global_temp` database.

#### Temporary vs. Global Temporary Views

- **Temporary Views**: Limited to a single Spark session.
- **Global Temporary Views**: Accessible across multiple sessions.

### Viewing the Metadata

- Spark manages metadata using the **Catalog**, which stores information about databases, tables, and views.

### Caching SQL Tables

- Like DataFrames, SQL tables and views can be cached for faster access.
- Spark 3.0 introduced **LAZY caching**, where a table is cached only when first used, instead of immediately.

### Reading Tables into DataFrames

- Data engineers often populate Spark SQL tables with cleansed data during ETL processes for downstream consumption.

## Data Sources for DataFrames and SQL Tables

### Parquet

- Default data source in Spark.
- Open-source columnar file format offering:
    - **Compression**: Reduces storage size.
    - **Fast Access**: Optimized for reading.
- Recommended for storing transformed and cleansed DataFrames.
- Default format for Delta Lake tables.

### JSON

- Popular data format known for its simplicity and ease of parsing.
- Two representational formats:
    - **Single-line mode**: Each line represents a single JSON object.
    - **Multiline mode**: Entire multiline text constitutes a single JSON object.
- To read in multiline mode, set `multiLine` to `true` in the `option()` method.

### CSV

- Common text file format with fields separated by commas (or other delimiters).
- Widely used for data exchange, especially in spreadsheets.
- Supports custom delimiters for cases where commas appear in the data.

### Avro

- Introduced in Spark 2.4 as a built-in data source.
- Used for serializing and deserializing messages (e.g., Apache Kafka).
- Benefits include:
    - **Direct JSON mapping**
    - **Efficiency**: Fast and lightweight.
    - **Cross-language support**: Compatible with various programming languages.

### ORC

- Columnar file format optimized for big data operations.
- Spark 2.x supports a **vectorized reader**, which:
    - Reads blocks of rows instead of one row at a time.
    - Improves performance in scans, filters, aggregations, and joins.
- To enable the vectorized reader:
    - Set `spark.sql.orc.impl` to `native`.
    - Set `spark.sql.orc.enableVectorizedReader` to `true`.
- For Hive ORC SerDe tables, set `spark.sql.hive.convertMetastoreOrc` to `true`.

### Images

- Introduced in Spark 2.4 to support machine learning frameworks like TensorFlow and PyTorch.
- Useful for computer vision tasks involving image datasets.

### Binary Files

- Added in Spark 3.0 as a data source.
- Converts each binary file into a DataFrame row with the following columns:
    - `path` (StringType)
    - `modificationTime` (TimestampType)
    - `length` (LongType)
    - `content` (BinaryType)