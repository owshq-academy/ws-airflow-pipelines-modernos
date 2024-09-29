# Ingestion Data Stores Application

> * application that generates data

### application structure
```shell
# 1) [objects] are the entities that generates data
# 2) [datastore] main entry for ingesting data into the data stores
```

### generate data using [cli]
```sh
# main
python3.9 cli.py

# apache kafka ~ strimzi
python3.9 cli.py 'strimzi-users-json'
python3.9 cli.py 'strimzi-users-json-ssl'
python3.9 cli.py 'strimzi-users-without-log-compaction-json'
python3.9 cli.py 'strimzi-users-with-log-compaction-json'
python3.9 cli.py 'strimzi-user-events-json'
python3.9 cli.py 'strimzi-flight-events-json'
python3.9 cli.py 'strimzi-agent-json'
python3.9 cli.py 'strimzi-credit-card-json'
python3.9 cli.py 'strimzi-musics-json'
python3.9 cli.py 'strimzi-movies-titles-json'
python3.9 cli.py 'strimzi-movies-keywords-json'
python3.9 cli.py 'strimzi-movies-ratings-json'
python3.9 cli.py 'strimzi-rides-json'
python3.9 cli.py 'strimzi-users-avro'
python3.9 cli.py 'strimzi-users-without-log-compaction-avro'
python3.9 cli.py 'strimzi-users-with-log-compaction-avro'
python3.9 cli.py 'strimzi-agent-avro'
python3.9 cli.py 'strimzi-credit-card-avro'
python3.9 cli.py 'strimzi-musics-avro'
python3.9 cli.py 'strimzi-rides-avro'

# apache kafka ~ skin using api manager [kong]
python3.9 cli.py 'skin-json'

# apache kafka ~ confluent cloud
python3.9 cli.py 'confluent-cloud-users-json'
python3.9 cli.py 'confluent-cloud-agent-json'
python3.9 cli.py 'confluent-cloud-musics-json'
python3.9 cli.py 'confluent-cloud-rides-json'

# apache pulsar
python3.9 cli.py 'pulsar-rides-json'

# real-time data ingestion
python3.9 cli.py 'eventhubs-music-events'
python3.9 cli.py 'pubsub-music-events'

# relational databases
python3.9 cli.py 'mssql'
python3.9 cli.py 'sqldb'
python3.9 cli.py 'postgres'
python3.9 cli.py 'ysql'
python3.9 cli.py 'mysql'

# nosql databases
python3.9 cli.py 'mongodb'
python3.9 cli.py 'cosmosdb-sqlapi'
python3.9 cli.py 'ycql'

# object stores
python3.9 cli.py 'minio'
python3.9 cli.py 'minio-movies'
python3.9 cli.py 'blob-storage'
```

### verify apache kafka events

```sh
# kcat cli
BROKER_IP=167.99.20.155:9094
kcat -C -b $BROKER_IP -t src-app-user-events-json -J -o end
kcat -C -b $BROKER_IP -t src-app-flight-events-json -J -o end
kcat -C -b $BROKER_IP -t src-app-ride-events-json -J -o end
```

### deploy [api] app on kubernetes

```sh
# init fast api locally
https://github.com/owshq-scorpius/owshq-svc-scorpius/blob/master/apps/ingestion-data-stores-app
uvicorn api:app --reload
http://127.0.0.1:8000/docs

# build image
docker build -f Dockerfile.api --tag ingestion-data-stores-app-python-api .
docker tag ingestion-data-stores-app-python-api owshq/ingestion-data-stores-app-python-api:1.0
docker push owshq/ingestion-data-stores-app-python-api:1.0

# access docker image
docker images
docker run --name ingestion-data-stores-app-python-api- --rm -i -t ingestion-data-stores-app-python-api bash

# get cluster context
kubectx do-nyc3-do-owshq-scorpius

# deploy app
https://github.com/owshq-scorpius/owshq-svc-scorpius/blob/master/apps/ingestion-data-stores-app/deployment/api
k apply -f deployment.yaml -n app
k apply -f service.yaml -n app

# deployment & svc info
kubens app
k describe deployment ingestion-data-stores-app-python-api
k describe svc svc-lb-ingestion-data-stores-app-python-api

# access fast api to trigger calls
# find load balancer
http://20.246.125.186:3636/docs
http://159.89.247.235:3636/docs

# housekeeping
k delete deployment ingestion-data-stores-app-python-api
k delete svc svc-lb-ingestion-data-stores-app-python-api
```