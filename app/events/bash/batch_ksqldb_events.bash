cd "/Users/luanmorenomaciel/BitBucket/applications/ingestion-data-stores-app"

for i in {1..10}
do
  python3.9 cli.py 'strimzi-musics-avro'
  python3.9 cli.py 'strimzi-credit-card-avro'
  python3.9 cli.py 'strimzi-agent-avro'
  python3.9 cli.py 'strimzi-users-avro'
done
