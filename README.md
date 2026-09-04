# Aula de PDM — GCP Pub/Sub e pipeline de anúncios

Material das aulas de Processamento de Dados Massivos (PDM) sobre serviços de dados da Google Cloud.
O conteúdo está em notebooks Jupyter, que rodam tanto nos notebooks do BigQuery Studio quanto no
JupyterLab de um cluster Dataproc.

## Notebooks

- **Aula 1 — `gcp-pubsub-v2.ipynb`**: tópicos, assinaturas, retenção e *seek*, assinatura do BigQuery
  com esquema e DLQ, assinatura do Cloud Storage em Avro. **É por aqui que você começa.**
- **Aula 2 — `gcp-pipeline-anuncios.ipynb`**: o pipeline completo de um anúncio. A coleta na API do
  Chaves na Mão, o texto seguindo pelo Pub/Sub até o BigQuery, as imagens seguindo para o Cloud
  Storage, e a extração de uma característica da imagem com o Gemini. Depende da infraestrutura criada
  na aula 1.
- `simple_crawler.py`: a coleta dos anúncios e o download das imagens. É o que a aula 2 importa.
- `subscriber.py` e `subscriber_with_seek.py`: os assinantes da seção 2 da aula 1 em formato de script,
  para rodar em um terminal ao lado do notebook — um assinante ativo bloqueia o kernel do Jupyter.
- `arquivo/`: material que não é mais usado em aula, guardado como referência. A edição de 2024
  (`gcp-pubsub-v1.ipynb`), o notebook de Dataflow e Apache Beam (`gcp-dataflow.ipynb`), os dois
  notebooks de crawler que a aula 2 absorveu, `publicacao-anuncios.ipynb` e `publisher.ipynb` — a
  publicação avulsa dos anúncios e um rascunho de publicação, ambos superados pela seção 6 da aula 1 —,
  e o `ml/` — a análise de imóveis com o Gemini via LangChain, com o prompt completo de avaliação que
  serve de ponto de partida para o trabalho de casa da aula 2.
- `tests/` e `requirements-dev.txt`: os testes automatizados do `simple_crawler.py` e as dependências
  usadas só no desenvolvimento do material — não entram em aula.

## Preparação do ambiente

**Notebooks do BigQuery Studio**: não há nada a preparar. Faça o upload do `gcp-pubsub-v2.ipynb` pelo
painel do BigQuery — o passo a passo, com imagens, está na primeira seção do próprio notebook.

**Cluster Dataproc**: a criação do cluster e a configuração de rede que lhe dá saída para a internet
estão em [`SETUP-DATAPROC.md`](SETUP-DATAPROC.md).

Em ambos, os dados de anúncios são obtidos por uma célula do próprio notebook; o `git clone` deixou de
ser necessário.

A aula 2 usa o Gemini pela Vertex AI, com a mesma credencial do notebook — não é preciso criar chave de
API. A única exigência é a API estar habilitada no projeto, o que a primeira célula da seção 4 faz:

```bash
gcloud services enable aiplatform.googleapis.com
```

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
