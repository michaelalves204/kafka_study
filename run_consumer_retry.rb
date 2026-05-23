# frozen_string_literal: true

require_relative 'consumer_retry'

# Executa o ConsumerRetry com suporte a Dead Letter Queue (DLQ).
#
# Após 3 tentativas falhas, a mensagem é enviada para o tópico "<topico>.dlq".
# Você precisa criar esse tópico antes de executar:
#
#   TOPIC=meu-topico.dlq ruby create_topic.rb
#
# Ou criar manualmente com Connection:
#   Connection.new.create.create_topic('meu-topico.dlq', num_partitions: 1, replication_factor: 2)
#
# Uso:
#   GROUP_ID=grupo-retry ruby run_consumer_retry.rb
#
# Ou com um group_id personalizado:
#   GROUP_ID=meu-grupo ruby run_consumer_retry.rb

group_id = ENV.fetch('GROUP_ID', 'grupo-retry')

ConsumerRetry.new(group_id: group_id).call
