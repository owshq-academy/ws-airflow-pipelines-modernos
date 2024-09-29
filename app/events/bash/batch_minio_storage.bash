cd "/Users/luanmorenomaciel/GitHub/owshq-svc-scorpius/apps/ingestion-data-stores-app"

for i in {1..1000}
do
   python3.9 cli.py 'minio'
done
