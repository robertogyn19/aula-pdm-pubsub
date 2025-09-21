# Aula de PDM sobre GCP Pub/Sub

Esse repositório tem um notebook com alguns exemplos práticos do GCP Pub/Sub.
A forma mais simples de consumir o conteúdo é importando diretmante no Jupyter do Dataproc.
Caso precise executar localmente, é necessário autenticar no GCP, você pode utilizar o comando abaixo.

```shell
gcloud auth application-default login
```

Caso esteja sem internet no dataproc, execute esse comando abaixo no terminal

```
wget https://storage.googleapis.com/aula-pdm-codigo/aula-pdm-pubsub-main.zip
unzip aula-pdm-pubsub-main.zip
```

Veja a [documentação oficial](https://googleapis.dev/python/google-api-core/latest/auth.html) para mais detalhes.

### Permissões

Caso queira deixar todas as permissões liberadas para o Pub/Sub, você pode executar o comando abaixo no Google Cloud
Shell.

```shell
# Obtém o ID do projeto
export PROJECT_ID=$(gcloud config get project)

# Obtém o número do projeto
export PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')

# Conta de serviço do Pub/Sub
export SA=$(echo "service-$PROJECT_NUMBER@gcp-sa-pubsub.iam.gserviceaccount.com")

# Concede as permissões necessárias
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA" \
  --role="roles/storage.admin"
```