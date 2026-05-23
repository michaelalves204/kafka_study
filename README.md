## Estudos sobre Kafka

### Setup do projeto

#### 1. Configurar variáveis de ambiente

Copie o arquivo de exemplo e ajuste se necessário:

```bash
cp .env.example .env
```

As variáveis disponíveis:

| Variável | Descrição | Exemplo |
|---|---|---|
| `TOPIC` | Nome do tópico Kafka | `my-topic` |
| `KAFKA_BROKERS` | Lista de brokers separada por vírgula | `localhost:9092,localhost:9094` |
| `CLIENT_ID` | Identificador do cliente Kafka | `my-app` |
| `GROUP_ID` | Grupo de consumo padrão | `my-group` |

#### 2. Subir o cluster Kafka

```bash
docker compose up --build
```

Isso inicia 2 brokers Kafka (kafka1 e kafka2) e o Kafka UI.

A interface de administração Kafka UI fica disponível em:

```
http://localhost:8080/
```

#### 3. Instalar as dependências

```bash
gem install bundler
bundle install
```

#### 4. Criar o tópico

```bash
ruby create_topic.rb
```

#### 5. (Opcional) Criar o tópico da Dead Letter Queue

```bash
TOPIC=my-topic.dlq ruby create_topic.rb
```

#### 6. Testar o envio de mensagens

```bash
ruby send_message.rb
# ou para enviar 5 mensagens de uma vez:
ruby send_messages.rb
```

#### 7. Consumir mensagens

**Consumidor simples (auto-commit):**

```bash
GROUP_ID=grupo-teste ruby consumer.rb
```

**Consumidor com retry e DLQ:**

```bash
GROUP_ID=grupo-retry ruby run_consumer_retry.rb
```

**Consumir mensagens da Dead Letter Queue:**

```bash
ruby run_dlq_consumer.rb
```

### Ambiente

O projeto utiliza um cluster Kafka com 2 brokers (kafka1 e kafka2) configurados via
**KRaft** (sem ZooKeeper), rodando em Docker Compose. A interface de administração
Kafka UI está disponível em `http://localhost:8080`.

### Conceitos relacionados ao Kafka

#### Topic → canal onde as mensagens são publicadas
#### Partition → divisão do topic para paralelismo
#### Consumer Group → grupo de consumidores; cada mensagem é processada por apenas um consumer do grupo
#### Broker → servidor Kafka que armazena os dados
#### Offset → posição da mensagem dentro da partition
#### Key → chave opcional que determina a partição da mensagem (mesma key → mesma partição, garantindo ordem)
#### Producer → publica mensagens em um tópico, podendo definir key e partição
#### Consumer → lê mensagens de um tópico, inscrito em um grupo de consumo

### Garantia de entrega

#### Auto-commit (padrão)
- O Kafka commita o offset automaticamente em intervalo fixo
- Garantia **at-most-once**: se o consumer cair entre o commit e o processamento, a mensagem é perdida

#### Offset manual
- Auto-commit desabilitado via `offset_commit_interval: 0`
- O offset só é commitado após `mark_message_as_processed` ser chamado
- Garantia **at-least-once**: se o consumer cair, o Kafka reentrega a mensagem não commitada

### Tolerância a falhas

#### Replication Factor → número de cópias de cada partição (aqui: 2)
- Se um broker cair, o outro assume as partições do líder
- Configurado em `docker-compose.yml` e `create_topic.rb`

### Padrões de resiliência

#### Retry → re-tentativas de processamento em caso de erro
- Máximo de 3 tentativas por mensagem
- Intervalo de 1 segundo entre tentativas
- Implementado no `ConsumerRetry`

#### Dead Letter Queue (DLQ) → tópico separado para mensagens que falharam
- Após esgotar as 3 tentativas, a mensagem é publicada no tópico `<topic>.dlq`
- Contém um envelope com metadados: tópico original, partição, offset, erro, timestamp
- Permite auditoria e reprocessamento sem travar o consumo normal
- Implementado no `ConsumerRetry`

### Arquivos do projeto

| Arquivo | Descrição |
|---|---|
| `connection.rb` | Conexão com o cluster Kafka (lê `KAFKA_BROKERS` e `CLIENT_ID`) |
| `producer.rb` | Publica mensagens no tópico configurado |
| `consumer.rb` | Consome mensagens com auto-commit (at-most-once) |
| `consumer_retry.rb` | Consome com offset manual (at-least-once), retry e DLQ |
| `send_message.rb` | Script rápido para publicar uma mensagem de teste |
| `send_messages.rb` | Publica 5 mensagens sequenciais para testar retry/DLQ |
| `create_topic.rb` | Cria o tópico configurado no .env |
| `docker-compose.yml` | Cluster Kafka com 2 brokers + Kafka UI |

### KRaft (Kafka Raft Metadata mode)

É o mecanismo do Kafka que substitui o ZooKeeper para gerenciar o cluster.
Ele usa o algoritmo de consenso Raft para manter os metadados do cluster sincronizados entre os nós.

**O que o KRaft faz?**
- Eleição de líder de controller
- Gerenciamento de tópicos
- Metadados de partitions
- Replicação
- Detecção de falhas
- Configuração do cluster