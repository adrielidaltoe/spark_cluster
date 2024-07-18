# Spark Cluster

# Starting the cluster in Docker
In the root folder run:
1. docker build -t {image_name} Dockerfile
2. docker compose up -d
3. To remove the containers: docker compose down

# Submitting an application
1. Access the spark-master container: docker exec -it spark-master /bin/bash
2. Run $SPARK_HOME/bin/spark-submit --deploy-mode client $SPARK_HOME/apps/{app_name.py} $SPARK_HOME/apps/data/sample.csv
