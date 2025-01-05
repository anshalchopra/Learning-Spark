# What Is Apache Spark?

Apache Spark is a unified engine designed for large-scale distributed data processing, on premises in data centers or in the cloud. Spark’s design philosophy centers around four key characteristics

### Speed

Spark has pursued the goal of speed in several ways. First, its internal implementation benefits immensely from the hardware industry’s recent huge strides in improving the price and performance of CPUs and memory. Today’s commodity servers come cheap, with hundreds of gigabytes of memory, multiple cores, and the underlying Unix-based operating system taking advantage of efficient multithreading and parallel processing. The framework is optimized to take advantage of all of these factors.

Second, Spark builds its query computations as a directed acyclic graph (DAG); its DAG scheduler and query optimizer construct an efficient computational graph that can usually be decomposed into tasks that are executed in parallel across workers on the cluster. 

And third, its physical execution engine, Tungsten, uses whole-stage code generation to generate compact code for execution (we will cover SQL optimization and whole-stage code generation in Chapter 3). With all the intermediate results retained in memory and its limited disk I/O, this gives it a huge performance boost.

### Ease of Use

Spark achieves simplicity by providing a fundamental abstraction of a simple logical data structure called a Resilient Distributed Dataset (RDD) upon which all other higher-level structured data abstractions, such as DataFrames and Datasets, are constructed. By providing a set of transformations and actions as operations, Spark offers a simple programming model that you can use to build big data applications in familiar languages.

### Modularity

Spark operations can be applied across many types of workloads and expressed in any of the supported programming languages: Scala, Java, Python, SQL, and R. Spark offers unified libraries with well-documented APIs that include the following modules as core components: Spark SQL, Spark Structured Streaming, Spark MLlib, and GraphX, combining all the workloads running under one engine.

### Extensibility

Spark focuses on its fast, parallel computation engine rather than on storage. Unlike Apache Hadoop, which included both storage and compute, Spark decouples the two. That means you can use Spark to read data stored in myriad sources—Apache Hadoop, Apache Cassandra, Apache HBase, MongoDB, Apache Hive, RDBMSs, and more—and process it all in memory. Spark’s DataFrameReaders and DataFrameWriters can also be extended to read data from other sources, such as Apache Kafka, Kinesis, Azure Storage, and Amazon S3, into its logical data abstraction, on which it can operate.

![Screenshot 2024-12-24 at 1.16.18 PM.png](https://prod-files-secure.s3.us-west-2.amazonaws.com/84678eb0-350d-410c-8506-c0b71020eafa/52f0e911-11ab-449d-9902-af1cd3d467dc/Screenshot_2024-12-24_at_1.16.18_PM.png)

# Unified Analytics

Spark offers four distinct components as libraries for diverse
workloads: Spark SQL, Spark MLlib, Spark Structured Streaming, and GraphX. Each of these components is separate from Spark’s core fault-tolerant engine, in that you use APIs to write your Spark application and Spark converts this into a DAG that is executed by the core engine. So whether you write your Spark code using the provided Structured APIs in Java, R, Scala, SQL, or Python, the underlying code is decomposed into highly compact bytecode that is executed in the workers’ JVMs across the cluster.

![Screenshot 2024-12-24 at 1.19.08 PM.png](https://prod-files-secure.s3.us-west-2.amazonaws.com/84678eb0-350d-410c-8506-c0b71020eafa/9d906692-c62b-437d-af6c-44d54c936640/Screenshot_2024-12-24_at_1.19.08_PM.png)

### Spark SQL

This module works well with structured data. You can read data stored in an RDBMS table or from file formats with structured data (CSV, text, JSON, Avro, ORC, Parquet, etc.) and then construct permanent or temporary tables in Spark. Also, when using Spark’s Structured APIs in Java, Python, Scala, or R, you can combine SQL-like queries to query the data just read into a Spark DataFrame. To date, Spark SQL is ANSI SQL:2003-compliant and it also functions as a pure SQL engine.

### Spark MLlib

Spark comes with a library containing common machine learning (ML) algorithms called MLlib. Since Spark’s first release, the performance of this library component has improved significantly because of Spark 2.x’s underlying engine enhancements. MLlib provides many popular machine learning algorithms built atop high-level DataFrame-based APIs to build models. These APIs allow you to extract or transform features, build pipelines (for training and evaluating), and persist models (for saving and reloading them) during deployment. Additional utilities include the use of common linear algebra operations and statistics. MLlib includes other low-level ML primitives, including a generic gradient descent optimization.

### Spark Structured Streaming

Apache Spark 2.0 introduced an experimental Continuous Streaming model and Structured Streaming APIs, built atop the Spark SQL engine and DataFrame based APIs. By Spark 2.2, Structured Streaming was generally available, meaning that developers could use it in their production environments. Necessary for big data developers to combine and react in real time to both static data and streaming data from engines like Apache Kafka and other streaming sources, the new model views a stream as a continually growing table, with new rows of data appended at the end. Developers can merely treat this as a structured table and issue queries against it as they would a static table. Underneath the Structured Streaming model, the Spark SQL core engine handles all aspects of fault tolerance and late data semantics, allowing developers to focus on writing streaming applications with relative ease.

### GraphX

As the name suggests, GraphX is a library for manipulating graphs (e.g., social network graphs, routes and connection points, or network topology graphs) and performing graph-parallel computations. It offers the standard graph algorithms for analysis, connections, and traversals, contributed by users in the community: the available algorithms include PageRank, Connected Components, and Triangle Counting

# Spark’s Distributed Execution

At a high level in the Spark architecture, a Spark application consists of a driver program that is responsible for orchestrating parallel operations on the Spark cluster. The driver accesses the distributed components in the cluster—the Spark executors and cluster manager through a SparkSession.

![Screenshot 2024-12-24 at 1.30.14 PM.png](https://prod-files-secure.s3.us-west-2.amazonaws.com/84678eb0-350d-410c-8506-c0b71020eafa/ccad5dad-e2e9-4848-a671-99581a0121d5/Screenshot_2024-12-24_at_1.30.14_PM.png)

### Spark Driver

As the part of the Spark application responsible for instantiating a SparkSession, the Spark driver has multiple roles: it communicates with the cluster manager; it requests resources (CPU, memory, etc.) from the cluster manager for Spark’s executors (JVMs); and it transforms all the Spark operations into DAG computations, schedules them, and distributes their execution as tasks across the Spark executors. Once the resources are allocated, it communicates directly with the executors.

### Spark Session

In Spark 2.0, the SparkSession became a unified conduit (medium or channel through which information flows) to all Spark operations and data. Through this one conduit, you can create JVM runtime parameters, define DataFrames and Datasets, read from data sources, access catalog metadata, and issue Spark SQL queries. SparkSession provides a single unified entry point to all of Spark’s functionality. In a standalone Spark application, you can create a SparkSession using one of the high-level APIs in the programming language of your choice. In the Spark shell the SparkSession is created for you, and you can
access it via a global variable called spark or sc.

### Cluster Manager

The cluster manager is responsible for managing and allocating resources for the cluster of nodes on which your Spark application runs. Currently, Spark supports four cluster managers: the built-in standalone cluster manager, Apache Hadoop YARN, Apache Mesos, and Kubernetes.

### Spark Executor

A Spark executor runs on each worker node in the cluster. The executors communicate with the driver program and are responsible for executing tasks on the workers. In most deployments modes, only a single executor runs per node.

![Screenshot 2024-12-24 at 2.03.44 PM.png](https://prod-files-secure.s3.us-west-2.amazonaws.com/84678eb0-350d-410c-8506-c0b71020eafa/a4a1e50c-dcd3-49eb-b752-fb054aab1797/Screenshot_2024-12-24_at_2.03.44_PM.png)

### Distributed Data & Partitions

Actual physical data is distributed across storage as partitions residing in either HDFS or cloud storage (see Figure 1-5). While the data is distributed as partitions across the physical cluster, Spark treats each partition as a high-level logical data abstraction—as a DataFrame in memory. Though this is not always possible, each Spark executor is preferably allocated a task that requires it to read the partition closest to it in the network, observing data locality.

![Screenshot 2024-12-24 at 2.08.01 PM.png](https://prod-files-secure.s3.us-west-2.amazonaws.com/84678eb0-350d-410c-8506-c0b71020eafa/0b4ba4a1-fd0c-49dd-b331-78cc646866ce/Screenshot_2024-12-24_at_2.08.01_PM.png)

Partitioning allows for efficient parallelism. A distributed scheme of breaking up data into chunks or partitions allows Spark executors to process only data that is close to them, minimizing network bandwidth. That is, each executor’s core is assigned its own data partition to work on

![Screenshot 2024-12-24 at 2.09.49 PM.png](https://prod-files-secure.s3.us-west-2.amazonaws.com/84678eb0-350d-410c-8506-c0b71020eafa/9dbe0f2e-1b6e-4153-8db8-1141bf7cb122/Screenshot_2024-12-24_at_2.09.49_PM.png)

# Spark Concepts

Application: A user program built on Spark using its APIs. It consists of a driver program and executors on the cluster.

SparkSession: An object that provides a point of entry to interact with underlying Spark func‐
tionality and allows programming Spark with its APIs. In an interactive Spark
shell, the Spark driver instantiates a SparkSession for you, while in a Spark
application, you create a SparkSession object yourself.

Job: A parallel computation consisting of multiple tasks that gets spawned in response
to a Spark action (e.g., save(), collect()).

Stage
Each job gets divided into smaller sets of tasks called stages that depend on each
other.

Task
A single unit of work or execution that will be sent to a Spark executor.

#### Spark Application and Spark Session

At the core of every Spark application is the Spark driver program, which creates a
SparkSession object. When you’re working with a Spark shell, the driver is part of
the shell and the SparkSession object (accessible via the variable spark) is created itself. Figure shows how Spark executes on a cluster once you’ve done this.
Once you have a SparkSession, you can program Spark using the APIs to perform
Spark operations.

#### Spark Jobs
During interactive sessions with Spark shells, the driver converts your Spark applica‐
tion into one or more Spark jobs. It then transforms each job into a
DAG. This, in essence, is Spark’s execution plan, where each node within a DAG
could be a single or multiple Spark stages.

#### Spark Stages
As part of the DAG nodes, stages are created based on what operations can be per‐
formed serially or in parallel (Figure 2-4). Not all Spark operations can happen in a
single stage, so they may be divided into multiple stages. Often stages are delineated
on the operator’s computation boundaries, where they dictate data transfer among
Spark executors.

#### Spark Tasks
Each stage is comprised of Spark tasks (a unit of execution), which are then federated
across each Spark executor; each task maps to a single core and works on a single par‐
tition of data (Figure 2-5). As such, an executor with 16 cores can have 16 or more
tasks working on 16 or more partitions in parallel, making the execution of Spark’s
tasks exceedingly parallel!

### Transformation, Actions and Lazy Evaluations
Spark operations on distributed data can be classified into two types: transformations
and actions. Transformations, as the name suggests, transform a Spark DataFrame
into a new DataFrame without altering the original data, giving it the property of
immutability. Put another way, an operation such as select() or filter() will not
change the original DataFrame; instead, it will return the transformed results of the
operation as a new DataFrame.
That is, their results are not computed imme‐
diately, but they are recorded or remembered as a lineage. A recorded lineage allows
Spark, at a later time in its execution plan, to rearrange certain transformations, coa‐
lesce them, or optimize transformations into stages for more efficient execution. Lazy
evaluation is Spark’s strategy for delaying execution until an action is invoked or data
is “touched” (read from or written to disk).
An action triggers the lazy evaluation of all the recorded transformations. In
Figure 2-6, all transformations T are recorded until the action A is invoked. Each
transformation T produces a new DataFrame.
While lazy evaluation allows Spark to optimize your queries by peeking into your
chained transformations, lineage and data immutability provide fault tolerance.
Because Spark records each transformation in its lineage and the DataFrames are
immutable between transformations, it can reproduce its original state by simply
replaying the recorded lineage, giving it resiliency in the event of failures.
he actions and transformations contribute to a Spark query plan, which we will
cover in the next chapter. Nothing in a query plan is executed until an action is
invoked. The following example, shown both in Python and Scala, has two transfor‐
mations—read() and filter()—and one action—count(). The action is what triggers the execution of all transformations recorded as part of the query execution
plan.

### Narrow and Wide Transformations
Transformations can be classified as having either narrow dependencies or wide
dependencies. Any transformation where a single output partition can be computed
from a single input partition is a narrow transformation. For example, in the previous
code snippet, filter() and contains() represent narrow transformations because
they can operate on a single partition and produce the resulting output partition
without any exchange of data.
However, groupBy() or orderBy() instruct Spark to perform wide transformations,
where data from other partitions is read in, combined, and written to disk. Since each
partition will have its own count of the word that contains the “Spark” word in its row
of data, a count (groupBy()) will force a shuffle of data from each of the executor’s
partitions across the cluster. In this transformation, orderBy() requires output from
other partitions to compute the final aggregation.


# Spark Structured API's

### What's Underneath an RDD?
The RDD is the most basic abstraction in Spark. There are three vital characteristics
associated with an RDD:
• Dependencies
• Partitions (with some locality information)
• Compute function: Partition => Iterator[T]

The original RDD model in Spark is simple and resilient, with features like dependencies for reproducibility, partitions for parallelism, and a compute function for generating data. However, its limitations include:
	1.	Opaque Computations: Spark treats compute functions (e.g., joins, filters) as black-box lambda expressions, limiting its ability to optimize them.
	2.	Opaque Data Types: For Python RDDs, Spark views data as generic objects, lacking knowledge of specific types or columns.
	3.	Inefficiency: Without understanding computations or data types, Spark cannot optimize expressions or leverage data compression.

These limitations prevent Spark from optimizing query plans effectively, prompting the need for higher-level APIs like DataFrames and Datasets, which address these issues by introducing schema-aware and optimized computations.

### Structuring Spark
Spark 2.x introduced a few key schemes for structuring Spark. One is to express com‐
putations by using common patterns found in data analysis. These patterns are
expressed as high-level operations such as filtering, selecting, counting, aggregating,
averaging, and grouping. This provides added clarity and simplicity.
This specificity is further narrowed through the use of a set of common operators in a
DSL. Through a set of operations in DSL, available as APIs in Spark’s supported lan‐
guages (Java, Python, Spark, R, and SQL), these operators let you tell Spark what you
wish to compute with your data, and as a result, it can construct an efficient query
plan for execution.