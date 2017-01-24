# Spark Scripts

The code in this repository is licensed under the [Apache License,
version 2](https://www.apache.org/licenses/LICENSE-2.0).
It depends Python and a POSIX shell.
Further, `process_logs.sh` depends on
[DagSim](https://github.com/eubr-bigsea/dagSim).

`process_logs.sh` extracts from experimental data the information about
Spark jobs, their stages and tasks.

`summarize.sh` performs a summarization of performance parameters
relative to Spark runs (more precisely relative to stages of such jobs).

## How to use the scripts

```shell
process_logs.sh [-p|-s|-h] directory
```

With `process_logs.sh` you can process experimental data obtained via
[Spark Experiment
Runner](https://github.com/deib-polimi/Spark-Experiment-Runner).
If run with the flag `-p`, it will only extract the compressed
archives and elaborate the logs.
On the other hand, using `-s` you can run a batch of simulations
with DagSim, starting from the already processed data.
If you do not use any flag, the script will do both steps.

```shell
summarize.sh [-u number] directory
```

Execute `summarize.sh` and pass the path to the root directory.
If you are considering multi-user experiments,
you can pass the `-u` option to provide this information.
Take into account that you should apply `process_logs.sh -p`
to `directory` beforehand.
