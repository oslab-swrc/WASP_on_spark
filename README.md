# Apache Spark with WASP

WASP is a workload-aware task scheduler and partitioner for in-memory MapReduce framework. Our scheduler contains:

- Analytical prediction model for predictioin of spark.default.parallelism and SPARK_WORKER_CORES parameters
- Runtime monitoring CPU utilization, spill, GCs
- Scheduler that maxmizes CPU utilization whil minimizing the overhead of data spills and GCs

WASP is implemented on Apache Spark (Link to specific version: https://github.com/apache/spark/tree/15de51c238a7340fa81cb0b80d029a05d97bfc5c).

WASP and Apache Spark both have Apache-2.0 license, as found in the [LICENSE](LICENSE) file.

## What is WASP
WASP jointly optimizes N<sub>partitions</sub> and N<sub>threads</sub> at runtime, which parameters are defined as:

- N<sub>partitions</sub>: how many data partitions are created from a single RDD (spark.default.parallelism)
- N<sub>threads</sub>: how many threads are allocated to a single executor (SPARK_WORKER_CORES)

Spark often suffers performance degradation with suboptimal N<sub>partitions</sub> and N<sub>threads</sub> parameters (e.g. typical guidelines suggest to use 2-3 tasks per CPU core for N<sub>threads</sub>) . Usually, these two parameters are set empirically by users, which yield suboptimal performance due to too high memory pressure or underutilization of concurrency. WASP firstly predicts N<sub>partitions</sub> and N<sub>threads</sub> with analystical models. And then, monitors memory pressure and concurrency at runtime and dynamically tunes the N<sub>partitions</sub> and N<sub>threads</sub>.
Thus, WASP achieves much faster execution time and high resource utilization compared to unoptimized Spark.

## How to Operate?
* Add 3 options in HiBench (or other configuration file)
  - spark.input.size: estimated data size in hadoop (or other DFS)
  - spark.total.executor.number: total number of executors in your cluster
  - spark.total.core.number: total number of cores in one executor

* Possible Spark version
  - 1.6.1, 2.0.1

* Possible benchmark
  - WordCount, Bayes, Kmeans, TeraSort, Sort, PageRank (HiBench v5.0)

## Demo Video
<img width="70%" src="WASP_demo.gif">

## Citation
Please cite the following paper if you use WASP:

**Jointly Optimizing Task Granularity and Concurrency for In-Memory MapReduce Frameworks.** Jonghyun Bae, Hakbeom Jang, Wenjing Jin, Jun Heo, Jaeyoung Jang, Joo-Young Hwang, Sangyeun Cho and Jae W. Lee. _Proceedings of the 2017 IEEE International Conference on Big Data (Big Data)_.

~~~
@INPROCEEDINGS{8257921,
  author={Bae, Jonghyun and Jang, Hakbeom and Jin, Wenjing and Heo, Jun and Jang, Jaeyoung and Hwang, Joo-Young and Cho, Sangyeun and Lee, Jae W.},
  booktitle={Proceedings of the 2017 IEEE International Conference on Big Data (Big Data)},
  title={Jointly optimizing task granularity and concurrency for in-memory mapreduce frameworks},
  year={2017},
  volume={},
  number={},
  pages={130-140},
  doi={10.1109/BigData.2017.8257921}}
~~~
