```sh
docker build -f Dockerfile.cli --tag ingestion-data-stores-cli-app .
docker tag ingestion-data-stores-cli-app owshq/ingestion-data-stores-cli-app:0.1
docker push owshq/ingestion-data-stores-cli-app:0.1
```

```sh
docker run -i -t ingestion-data-stores-cli-app /bin/bash
docker run ingestion-data-stores-cli-app python3.9 cli.py
```

```sh
# select k8s cluster
kubectx ovh-owshq-orion

# cron jobs 
k apply -f crj_mongodb.yaml -n app
k apply -f crj_mssql.yaml -n app
k apply -f crj_mysql.yaml -n app
k apply -f crj_postgres.yaml -n app
k apply -f crj_minio.yaml -n app
k apply -f crj_strimzi.yaml -n app
k apply -f crj_music_data.yaml -n app
k apply -f crj_users_data.yaml -n app
k apply -f crj_credit_card_data.yaml -n app
k apply -f crj_agent_data.yaml -n app
k apply -f crj_movies_titles.yaml -n app
k apply -f crj_movies_keywords.yaml -n app
k apply -f crj_movies_ratings.yaml -n app

# remove deployment
k delete cronjob crj-mongodb-ingestion
k delete cronjob crj-mssql-ingestion
k delete cronjob crj-mysql-ingestion
k delete cronjob crj-postgres-ingestion
k delete cronjob crj-minio-ingestion
k delete cronjob crj-music-data-ingestion
k delete cronjob crj-users-data-ingestion
k delete cronjob crj-credit-card-data-ingestion
k delete cronjob crj-agent-data-ingestion
k delete cronjob crj-movies-titles-ingestion
k delete cronjob crj-movies-keywords-ingestion
k delete cronjob crj-movies-ratings-ingestion
```