# dbt Core & Google BigQuery

### install and configure
```shell
pip install "dbt-core"
pip install "dbt-bigquery"

pip list | grep dbt

dbt --version

pip uninstall dbt-core
```

### execute models
```sh
dbt run
```

### docs
```sh
dbt docs generate
dbt docs serve --port 8000
```
