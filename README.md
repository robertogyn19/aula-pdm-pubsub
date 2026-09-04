# Aula de PDM — GCP Pub/Sub e Dataflow

Material das aulas de Processamento de Dados Massivos (PDM) sobre serviços de dados da Google Cloud.
O conteúdo está em notebooks Jupyter, que rodam tanto nos notebooks do BigQuery Studio quanto no
JupyterLab de um cluster Dataproc.

## Notebooks

- **Aula 1 — `gcp-pubsub-v2.ipynb`**: tópicos, assinaturas, retenção e *seek*, assinatura do BigQuery
  com esquema e DLQ, assinatura do Cloud Storage em Avro. **É por aqui que você começa.**
- **Aula 2 — `gcp-dataflow.ipynb`**: Apache Beam, o exemplo de *wordcount* e a execução do mesmo
  pipeline com o `DirectRunner` e com o `DataflowRunner`.
- `publicacao-anuncios.ipynb`: versão avulsa da publicação dos anúncios, para ambientes com terminal.
  Nos notebooks do BigQuery Studio, use a seção 6 da aula 1, que já traz essas células.
- `crawler-dados.ipynb` e `simple_crawler.py`: como os dados de anúncios foram coletados. O notebook
  importa o script, que é onde a lógica de fato vive.
- `crawler-download-imagens.ipynb`: download das imagens dos anúncios coletados.
- `ml/`: análise dessas imagens com o Gemini. Veja o [`ml/README.md`](ml/README.md), que explica como
  criar a chave de API.
- `subscriber.py` e `subscriber_with_seek.py`: os assinantes da seção 2 da aula 1 em formato de script,
  para rodar em um terminal ao lado do notebook — um assinante ativo bloqueia o kernel do Jupyter.
- `arquivo/`: material que não é mais usado em aula, guardado como referência — a edição de 2024
  (`gcp-pubsub-v1.ipynb`, com dados de clientes e vendas em CSV) e um rascunho de publicação.

## Preparação do ambiente

**Notebooks do BigQuery Studio**: não há nada a preparar. Faça o upload do `gcp-pubsub-v2.ipynb` pelo
painel do BigQuery — o passo a passo, com imagens, está na primeira seção do próprio notebook.

**Cluster Dataproc**: a criação do cluster e a configuração de rede que lhe dá saída para a internet
estão em [`SETUP-DATAPROC.md`](SETUP-DATAPROC.md).

Em ambos, os dados de anúncios são obtidos por uma célula do próprio notebook; o `git clone` deixou de
ser necessário.

## Execução local

Rodar localmente também funciona, mas exige autenticação no GCP:

```bash
gcloud auth application-default login
```

Os notebooks descobrem o projeto pela credencial (`google.auth.default()`), então confira antes que a
conta e o projeto ativos são os que você quer usar:

```bash
gcloud config list
```

Veja a [documentação oficial](https://googleapis.dev/python/google-api-core/latest/auth.html) para
mais detalhes sobre autenticação.

## Permissões

O Pub/Sub precisa de permissão para escrever no BigQuery e no Cloud Storage. Isso é feito por células do
próprio `gcp-pubsub-v2.ipynb`, nas seções 4.1, 4.4 e 7.2 — não é preciso rodar nada por fora.
