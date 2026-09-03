# Preparação do cluster Dataproc

Os notebooks das duas aulas rodam no JupyterLab de um cluster Dataproc. Este documento é o passo zero:
como criar o cluster e garantir que ele tenha saída para a internet.

## Por que a internet é um passo à parte

O cluster precisa de dois acessos diferentes, e eles não são a mesma coisa:

- **APIs do Google** (Dataproc, Pub/Sub, BigQuery, Storage): a VM alcança essas APIs mesmo sem IP externo.
- **Internet aberta** (PyPI e GitHub): exige que a VM tenha um IP externo ou que o projeto tenha um
  Cloud NAT.

O `gcloud dataproc clusters create` não garante o segundo. Quando nem `--no-address` nem
`--public-ip-address` são passados, a documentação do comando avisa que *"the Dataproc service will apply
a default policy to determine if each instance in the cluster gets an external IP address or not"*.

Quando essa política resulta em VM sem IP externo e o projeto não tem NAT, o efeito é traiçoeiro: o
cluster cria normalmente, o JupyterLab abre pelo Component Gateway e está tudo aparentemente certo — até
a primeira célula de `%pip install` ou o `git clone`, que ficam pendurados até dar timeout. Foi o que
aconteceu na aula de 2025.

A seção 2 resolve isso pedindo o IP externo de forma explícita, em vez de depender dessa política. Se o
projeto **proibir** IPs externos, o caminho é a alternativa da seção 6.

## 1. Configuração do projeto (uma vez, pelo professor)

Os alunos não precisam — e normalmente não têm permissão — para rodar esta seção. Uma vez habilitadas, as
APIs valem para todo mundo no projeto.

Os comandos `gcloud` deste documento podem ser rodados no Cloud Shell, o terminal embutido no console:
clique no ícone de terminal no canto superior direito e uma janela abre na parte de baixo da tela.

![Cloud Shell](imagens-dataflow/img2-cloud-shell.png)

```bash
export PROJECT_ID=<projeto-da-aula>

gcloud services enable \
    dataproc.googleapis.com \
    compute.googleapis.com \
    dataflow.googleapis.com \
    pubsub.googleapis.com \
    bigquery.googleapis.com \
    --project $PROJECT_ID
```

Pela interface, cada API é habilitada na própria página do produto:

![Ativar a Dataflow API](imagens-dataflow/img1-enable.png)

## 2. Criação do cluster (cada aluno cria o seu)

<!-- print do ensaio: formulário de criação do cluster no console -->
<!-- print do ensaio: seção de rede do formulário, com a opção de IP interno DESMARCADA -->
<!-- print do ensaio: lista de clusters com o status Executando -->

O ponto que decide se a aula vai funcionar está na configuração de rede do formulário: a opção de deixar
as instâncias **somente com IP interno** tem que ficar **desmarcada**. É ela que determina se o cluster
enxerga o PyPI e o GitHub.

Vale a pena conferir o botão de **linha de comando equivalente**, no fim do formulário: ele mostra o
comando `gcloud` correspondente ao que você preencheu na tela.

Pelo terminal, o mesmo cluster sai com:

```bash
gcloud dataproc clusters create $USER \
    --region us-central1 \
    --zone us-central1-a \
    --subnet=default \
    --public-ip-address \
    --delete-max-idle=120m \
    --single-node \
    --master-machine-type n2-standard-4 \
    --master-boot-disk-size 50GB \
    --image-version 2.2-debian12 \
    --enable-component-gateway \
    --optional-components=JUPYTER,ICEBERG \
    --project <projeto-da-aula>
```

O `--public-ip-address` está explícito de propósito: é ele que garante a saída para a internet, sem
depender da política padrão do serviço.

O `--delete-max-idle=120m` apaga o cluster sozinho depois de 2 horas ocioso, para ninguém esquecer a
máquina ligada.

## 3. Acessando o JupyterLab

O cluster é criado com o Component Gateway, então o Jupyter é acessado pelo console, sem IP externo na sua
máquina nem túnel SSH: abra a página do [Dataproc](https://console.cloud.google.com/dataproc/clusters),
clique no cluster, vá na aba `Interfaces da Web / Web Interfaces` e escolha `JupyterLab`.

<!-- print do ensaio: aba Interfaces da Web com o link do JupyterLab -->

## 4. Conferindo que o cluster tem internet

Já no JupyterLab, rode esta célula **antes** de qualquer outra coisa:

```python
!curl -sS -o /dev/null -w "%{http_code}\n" https://pypi.org/simple/
```

O resultado esperado é `200`. Se a célula ficar pendurada e terminar em erro de conexão, o cluster subiu
sem IP externo — refaça a seção 2 conferindo a opção de rede, ou use a alternativa da seção 6.

Os sintomas de um cluster sem saída para a internet são sempre os mesmos:

- `pip install` parado em `Retrying (Retry(total=4, ...)) after connection broken`;
- `git clone` parado em `Cloning into 'aula-pdm-pubsub'...` até dar timeout;
- os dois falham, mas as chamadas ao Pub/Sub e ao BigQuery funcionam, o que faz a falha parecer
  qualquer outra coisa.

## 5. Apagando o cluster

Ao fim da aula, apague o cluster para não acumular custo:

```bash
gcloud dataproc clusters delete $USER --region us-central1 --project <projeto-da-aula>
```

## 6. Alternativa: projeto que proíbe IP externo

Organizações costumam ter uma política (`constraints/compute.vmExternalIpAccess`) que impede VMs com IP
externo. Nesse caso a seção 2 falha, e a saída para a internet passa a depender de um Cloud NAT — que
também é configurado **uma vez, pelo professor**:

```bash
export PROJECT_ID=<projeto-da-aula>
export REGION=us-central1

# Deixa a VM alcançar as APIs do Google sem IP externo
gcloud compute networks subnets update default \
    --region=$REGION \
    --enable-private-ip-google-access \
    --project $PROJECT_ID

# Dá a saída para a internet aberta
gcloud compute routers create roteador-aula-pdm \
    --network=default --region=$REGION --project $PROJECT_ID

gcloud compute routers nats create nat-aula-pdm \
    --router=roteador-aula-pdm \
    --region=$REGION \
    --auto-allocate-nat-external-ips \
    --nat-all-subnet-ip-ranges \
    --project $PROJECT_ID
```

Com o NAT no lugar, o cluster da seção 2 passa a ser criado com `--no-address` no lugar de
`--public-ip-address` (ou, no formulário, com a opção de IP interno **marcada**). O NAT leva um ou dois
minutos para propagar e é cobrado por hora enquanto existir.
