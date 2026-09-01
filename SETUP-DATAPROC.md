# Preparação do cluster Dataproc

Os notebooks das duas aulas rodam no Jupyter de um cluster Dataproc. Este documento é o passo zero:
como criar o cluster e, principalmente, como garantir que ele tenha saída para a internet.

## Por que a internet é um passo à parte

O cluster precisa de dois acessos diferentes, e eles são resolvidos de formas diferentes:

- **APIs do Google** (Dataproc, Pub/Sub, BigQuery, Storage): funcionam sem IP externo, através do
  Private Google Access.
- **Internet aberta** (PyPI e GitHub): precisa de IP externo na VM ou de um Cloud NAT no projeto.

O `gcloud dataproc clusters create` não garante o segundo. Quando o `--no-address` é omitido, a própria
documentação do comando avisa que *"the Dataproc service will apply a default policy to determine if each
instance in the cluster gets an external IP address or not"*.

Quando essa política resulta em VM sem IP externo e o projeto não tem NAT, o efeito é traiçoeiro: o
cluster cria normalmente, o JupyterLab abre pelo Component Gateway e está tudo aparentemente certo — até
a primeira célula de `%pip install` ou o `git clone`, que ficam pendurados até dar timeout.

A solução é configurar um Cloud NAT no projeto **uma única vez**. Depois disso, todo cluster criado ali
tem internet, com ou sem IP externo.

## 1. Configuração do projeto (uma vez, pelo professor)

Todos os comandos desta seção podem ser rodados no [Cloud Shell](https://console.cloud.google.com).

![Cloud Shell](imagens-dataflow/img2-cloud-shell.png)

```bash
export PROJECT_ID=<projeto-da-aula>
export REGION=us-central1
```

### 1.1. Habilitar as APIs

```bash
gcloud services enable dataproc.googleapis.com compute.googleapis.com \
    --project $PROJECT_ID
```

A API do Dataflow, usada na aula 2, é habilitada da mesma forma ou pela interface:

![Ativar a Dataflow API](imagens-dataflow/img1-enable.png)

```bash
gcloud services enable dataflow.googleapis.com --project $PROJECT_ID
```

### 1.2. Private Google Access na sub-rede

É o que permite ao cluster falar com as APIs do Google sem IP externo:

```bash
gcloud compute networks subnets update default \
    --region=$REGION \
    --enable-private-ip-google-access \
    --project $PROJECT_ID
```

### 1.3. Cloud NAT

É o que dá saída para a internet aberta, que é o passo que faltou na aula de 2025:

```bash
gcloud compute routers create roteador-aula-pdm \
    --network=default \
    --region=$REGION \
    --project $PROJECT_ID

gcloud compute routers nats create nat-aula-pdm \
    --router=roteador-aula-pdm \
    --region=$REGION \
    --auto-allocate-nat-external-ips \
    --nat-all-subnet-ip-ranges \
    --project $PROJECT_ID
```

<!-- print do ensaio: tela do Cloud NAT criado no console -->

### 1.4. Conferência

```bash
# Deve imprimir True
gcloud compute networks subnets describe default \
    --region=$REGION --project $PROJECT_ID \
    --format="get(privateIpGoogleAccess)"

# Deve listar o nat-aula-pdm
gcloud compute routers nats list \
    --router=roteador-aula-pdm --region=$REGION --project $PROJECT_ID
```

## 2. Criação do cluster (cada aluno cria o seu)

```bash
gcloud dataproc clusters create $USER \
    --region us-central1 \
    --zone us-central1-a \
    --subnet=default \
    --no-address \
    --delete-max-idle=120m \
    --single-node \
    --master-machine-type n2-standard-4 \
    --master-boot-disk-size 50GB \
    --image-version 2.2-debian12 \
    --enable-component-gateway \
    --optional-components=JUPYTER,ICEBERG \
    --project <projeto-da-aula>
```

O `--no-address` está explícito de propósito: com o NAT da seção 1.3 a VM não precisa de IP externo, e
declarar isso evita depender da política padrão do serviço, que é o que variou de aluno para aluno.

O `--delete-max-idle=120m` apaga o cluster sozinho depois de 2 horas ocioso, para ninguém esquecer a
máquina ligada.

## 3. Conferindo que o cluster tem internet

Depois de abrir o JupyterLab, rode esta célula **antes** de qualquer outra coisa:

```python
!curl -sS -o /dev/null -w "%{http_code}\n" https://pypi.org/simple/
```

O resultado esperado é `200`. Se a célula ficar pendurada e terminar em erro de conexão, falta o NAT da
seção 1.3 — e é melhor descobrir isso agora do que no meio da aula.

Os sintomas de um cluster sem saída para a internet são sempre os mesmos:

- `pip install` parado em `Retrying (Retry(total=4, ...)) after connection broken`;
- `git clone` parado em `Cloning into 'aula-pdm-pubsub'...` até dar timeout;
- os dois falham, mas as chamadas ao Pub/Sub e ao BigQuery funcionam, porque essas passam pelo Private
  Google Access.

## 4. Acessando o JupyterLab

O cluster é criado com `--enable-component-gateway`, então o Jupyter é acessado pelo console, sem
precisar de IP externo nem de túnel SSH: abra a página do
[Dataproc](https://console.cloud.google.com/dataproc/clusters), clique no cluster, vá na aba
`Interfaces da Web / Web Interfaces` e escolha `JupyterLab`.

<!-- print do ensaio: aba Interfaces da Web com o link do JupyterLab -->

## 5. Apagando o cluster

Ao fim da aula, apague o cluster para não acumular custo:

```bash
gcloud dataproc clusters delete $USER --region us-central1 --project <projeto-da-aula>
```
