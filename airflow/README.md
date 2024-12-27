# [Airflow](https://spark.apache.org)

In this tutorial, you'll be running self-contained Airflow DAGs for a hypothetical food delivery service.
The food delivery service consists of models for food delivery routes, orders, and driver activity.
Our team is based in SF, so naturally, we'll be delivering food in the Bay Area.

## Requirements

* [Docker Compose](https://docs.docker.com/compose/install)
* [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview/)

## Oleander API Key

Go to [`https://oleander.dev`](https://oleander.dev), then copy you API key **under** `Settings` > `Account` > `API Key`.
In the step below, replace `[OLEANDER-API-KEY]` with your API key before running the Spark job.

## `.env`

```
cp .example.env .env
```

## Running the Airflow DAGs

First, start the database for our hypothetical food delivery service:

```
docker compose up
```

Then, start Airflow:

```
astro dev start
```

Finally, go to:
* [`http://localhost:8080`](http://localhost:8080) to view your Airflow DAGs.
* [`https://oleander.dev`](https://oleander.dev) to view the OpenLineage events emitted by your Airflow DAGs.