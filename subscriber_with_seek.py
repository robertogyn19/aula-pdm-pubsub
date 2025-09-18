from datetime import datetime, timedelta, timezone

import google.auth
from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1
from google.protobuf import timestamp_pb2
from google.pubsub_v1.types import PubsubMessage

_, project_id = google.auth.default()
print(project_id)

# Criação do cliente de publicação, com ele é possível criar tópicos e publicar mensagens
publisher = pubsub_v1.PublisherClient()

# Os nomes dos tópicos seguem o formato projects/<nome-do-projeto>/topics/<nome-do-tópico>
# o código abaixo configura esse nome
topic_name = "projects/{project_id}/topics/{topic}".format(
    project_id=project_id,
    topic="aula-pdm-primeiro-topico",
)

# Configuramos o nome da assinatura para o formato esperado assim como fizemos com o tópico
subscription_name = "projects/{project_id}/subscriptions/{sub}".format(
    project_id=project_id,
    sub="aula-pdm-minha-primeira-assinatura",
)


# A função abaixo será responsável por receber a mensagem
def callback(message: PubsubMessage):
    print(f"data: {message.data.decode('utf-8')} | attributes: {message.attributes}")
    # Essa chamada do ack é como o Pub/Sub controla quais mensagens foram processadas
    message.ack()


# Criação do subscriber, com ele é possível criar assinaturas e "se inscrever" em tópicos
subscriber = pubsub_v1.SubscriberClient()

# Criação da assinatura.
try:
    subscriber.create_subscription(
        name=subscription_name, topic=topic_name
    )
    print(f"O subscriber {subscription_name} criado com sucesso")
except AlreadyExists:
    print(f"A assinatura {subscription_name} já existe")

# 4) Fazer SEEK para 10 minutos atrás
#   (isso reposiciona o ponteiro da subscription para reentregar mensagens
#    publicadas desde esse instante, respeitando a retenção configurada)
dez_min_atras = datetime.now(timezone.utc) - timedelta(minutes=10)
ts = timestamp_pb2.Timestamp()
ts.FromDatetime(dez_min_atras)

subscriber.seek(request={"subscription": subscription_name, "time": ts})
print(f"Seek realizado para: {dez_min_atras.isoformat()}")

# A chamada abaixo registra a assinatura com a função de callback
# Dessa forma, toda mensagem que chegar no tópico da assinatura, executará o código cadastrado
future = subscriber.subscribe(subscription_name, callback)

# Assim como na publicação, a função result é para aguardar algo
# Ao executar o código abaixo, será que vai retornar algo?
# Como as mensagens chegam na função?
# E por quanto tempo devemos esperar por novas mensagens?
try:
    future.result()
except KeyboardInterrupt:
    print(f"Future cancelado")
    future.cancel()
