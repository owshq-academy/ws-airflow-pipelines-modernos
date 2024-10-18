curl -X POST "http://localhost:8080/api/v1/dags/airbyte-sync-astro-sdk-bq/dagRuns" \
-u "admin:admin" \
-H "Content-Type: application/json" \
-d '{
      "dag_run_id": "manual_run_airbyte-sync-astro-sdk-bq_'"$(date +%Y%m%d%H%M%S)"'"
    }'
