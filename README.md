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
