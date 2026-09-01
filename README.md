# Aula de PDM — GCP Pub/Sub e Dataflow

Material das aulas de Processamento de Dados Massivos (PDM) sobre serviços de dados da Google Cloud.
O conteúdo está em notebooks Jupyter, pensados para rodar em um cluster do Dataproc.

## Notebooks

- **Aula 1 — `gcp-pubsub-v2.ipynb`**: tópicos, assinaturas, retenção e *seek*, assinatura do BigQuery
  com esquema e DLQ, assinatura do Cloud Storage em Avro. **É por aqui que você começa.**
- **Aula 2 — `gcp-dataflow.ipynb`**: Apache Beam, o exemplo de *wordcount* e a execução do mesmo
  pipeline com o `DirectRunner` e com o `DataflowRunner`.
- `publicacao-anuncios.ipynb`: publica os anúncios no tópico, usado na seção 6 da aula 1.
- `subscriber.py` e `subscriber_with_seek.py`: os assinantes da seção 2 da aula 1 em formato de script,
  para rodar em um terminal ao lado do notebook — um assinante ativo bloqueia o kernel do Jupyter.
- `crawler-dados.ipynb` e `simple_crawler.py`: como os dados de anúncios foram coletados. O notebook
  importa o script, que é onde a lógica de fato vive.
- `arquivo/`: material que não é mais usado em aula, guardado como referência — a edição de 2024
  (`gcp-pubsub-v1.ipynb`, com dados de clientes e vendas em CSV) e um rascunho de publicação.

## Preparação do ambiente

As aulas rodam no JupyterLab de um cluster Dataproc. A criação do cluster e a configuração de rede que
lhe dá saída para a internet estão em [`SETUP-DATAPROC.md`](SETUP-DATAPROC.md) — comece por lá.

Com o cluster no ar, clone o repositório dentro dele e descompacte os dados:

```bash
cd /home/dataproc
git clone https://github.com/robertogyn19/aula-pdm-pubsub.git
cd aula-pdm-pubsub

unzip dados/anuncios.zip -d dados
```

Confira se a versão da biblioteca do Pub/Sub é igual ou superior a `2.27.0`, que é onde a
configuração de assinatura do BigQuery usada na aula 1 está disponível:

```bash
pip list | grep google-cloud-pubsub
```

Se for anterior, atualize com `pip install --upgrade google-cloud-pubsub`.

## Execução local

Rodar localmente também funciona, mas exige autenticação no GCP:

```bash
gcloud auth application-default login
```

Os notebooks descobrem o projeto pela credencial (`google.auth.default()`), então confira antes que
a conta e o projeto ativos são os que você quer usar:

```bash
gcloud config list
```

Veja a [documentação oficial](https://googleapis.dev/python/google-api-core/latest/auth.html) para
mais detalhes sobre autenticação.
