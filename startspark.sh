#!/bin/bash
export PYSPARK_PYTHON=python2.7
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-slave.sh spark://$(hostname):7077