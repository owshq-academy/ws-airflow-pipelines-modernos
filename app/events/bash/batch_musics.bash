cd "/Users/luanmorenomaciel/BitBucket/owshq-svc-scorpius/2-apps/ingestion-data-stores-app"

for i in {1..100}
do
   python3.9 cli.py 'strimzi-musics-json'
done
