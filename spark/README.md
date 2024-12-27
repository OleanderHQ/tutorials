# Simple [Spark](https://spark.apache.org) Job with [Iceberg](https://iceberg.apache.org)

In this tutorial, you'll be running a simple self-contained Spark application with Iceberg locally via Docker Compose.
We'll use an existing example iceberg table `demo.nyc.taxis` used in the [Iceberg quickstart guide](https://iceberg.apache.org/spark-quickstart/) for Spark.
Our team is based in SF, so naturally, we'll be using `demo.sf.waymo`.

## Requirements

* [Docker Compose](https://docs.docker.com/compose/install)

## Oleander API Key

Go to [`https://oleander.dev`](https://oleander.dev), then copy you API key **under** `Settings` > `Account` > `API Key`.
In the step below, replace `[OLEANDER-API-KEY]` with your API key before running the Spark job. 

## Running the Spark Job

Next, start up the docker containers with:

```
docker-compose up
```

Then, run:

```
docker exec -it spark-iceberg /opt/spark/bin/spark-submit \
  --conf spark.jars.packages=io.openlineage:openlineage-spark_2.12:1.26.0 \
  --conf spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener \
  --conf spark.openlineage.transport.type=http \
  --conf spark.openlineage.transport.url=https://oleander.dev \
  --conf spark.openlineage.transport.auth.type=api_key \
  --conf spark.openlineage.transport.auth.apiKey=[OLEANDER-API-KEY] \
  --conf spark.openlineage.namespace=spark-with-iceberg \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=$PWD/warehouse \
  --conf spark.sql.defaultCatalog=local \
  spark-with-iceberg.py
```

Finally, go to [`https://oleander.dev`](https://oleander.dev) to view the OpenLineage events emitted by your Spark job.