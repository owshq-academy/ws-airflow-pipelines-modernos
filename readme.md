# Desenvolvendo Pipelines de Dados Modernos com Apache Airflow

### astro cli install & init
```shell
https://www.astronomer.io/docs/astro/cli/overview
https://github.com/astronomer/astro-cli

brew install astro
astro dev init
```

### configure gcp env
```shell
gcloud auth login
gcloud projects list

gcloud config set project silver-charmer-243611

gcloud iam service-accounts create astro-cli-sa

gcloud projects add-iam-policy-binding silver-charmer-243611 \
  --member "serviceAccount:astro-cli-sa@silver-charmer-243611.iam.gserviceaccount.com" \
  --role "roles/editor"
  
gcloud iam service-accounts keys create gcp_key_file.json \
  --iam-account astro-cli-sa@silver-charmer-243611.iam.gserviceaccount.com

export GOOGLE_APPLICATION_CREDENTIALS=~/gcp_key_file.json
```

### init project
```shell
astro dev start
astro dev restart
```

### install libs
```shell
pip install apache-airflow
pip install astro-sdk-python
```