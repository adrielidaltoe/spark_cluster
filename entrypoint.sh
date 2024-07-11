#!/bin/bash

# Start Spark master
if [ "$SPARK_MODE" = "master" ]; then
    $SPARK_HOME/sbin/start-master.sh -h 0.0.0.0
fi

# Start Spark worker
if [ "$SPARK_MODE" = "worker" ]; then
    $SPARK_HOME/sbin/start-worker.sh $SPARK_MASTER_URL
fi

# Keep the container running
tail -f $SPARK_HOME/logs/spark-$hostname-org.apache.spark.deploy.master.Master-*.out
# $SPARK_HOME/logs/spark--org.apache.spark.deploy.master.Master-1-5b94f4cda072.out

#Testar esse container com logs