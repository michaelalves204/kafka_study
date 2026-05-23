# frozen_string_literal: true

require 'dotenv/load'
require 'kafka'
require 'json'
require_relative 'connection'

# ConsumerRetry processa mensagens com suporte a retry e Dead Letter Queue (DLQ).
#
# Quando uma mensagem falha após o número máximo de tentativas, ela é
# publicada em um tópico de erros (DLQ) para análise posterior, em vez de
# ser simplesmente descartada.
#
# A abordagem usa o mecanismo nativo do Kafka:
#   1. Auto-commit desabilitado
#   2. Offset só é commitado após processamento bem-sucedido
#   3. Se o consumidor cair, o Kafka reentrega a mensagem
#   4. Após 3 tentativas falhas, a mensagem vai para a DLQ
#
# @example Uso básico
#   ConsumerRetry.new(group_id: 'grupo-retry').call
class ConsumerRetry
  # @return [Integer] número máximo de tentativas antes de enviar para DLQ
  MAX_RETRIES = 3

  # Nome do tópico de Dead Letter Queue onde mensagens com falha são enviadas.
  # Segue o padrão "<tópico-original>.dlq"
  # @return [String]
  DLQ_TOPIC = "#{ENV.fetch('TOPIC')}.dlq"

  # Inicializa o consumidor com grupo de consumo e desabilita auto-commit.
  #
  # Cria um cliente Kafka, um consumer com offset manual e um producer
  # dedicado para publicar na DLQ.
  #
  # @param group_id [String] identificador do grupo de consumidores
  def initialize(group_id:)
    kafka_client = Connection.new.create

    @consumer = kafka_client.consumer(
      group_id: group_id,
      offset_commit_interval: 0,
      offset_commit_threshold: 0
    )

    @consumer.subscribe(ENV.fetch('TOPIC'))
    @dlq_producer = kafka_client.producer
  end

  # Inicia o loop de consumo com retry e DLQ.
  #
  # Bloqueia indefinidamente processando mensagens. Em caso de erro,
  # a mensagem é retentada até {MAX_RETRIES} vezes e então enviada
  # para o tópico DLQ.
  #
  # @return [void]
  def call
    print_startup_info

    @consumer.each_message { |message| process_with_retry(message) }
  rescue Interrupt
    puts "\nEncerrando consumer..."
    @consumer.stop
    @dlq_producer.shutdown
  end

  private

  # Printa informações iniciais do consumidor no terminal.
  #
  # @return [void]
  def print_startup_info
    puts 'Aguardando mensagens (retry + DLQ)...'
    puts "Máximo de tentativas por mensagem: #{MAX_RETRIES}"
    puts "DLQ topic: #{DLQ_TOPIC}"
    puts 'Offset só é commitado após sucesso no processamento.'
    puts
  end

  # Processa uma mensagem com lógica de retry.
  #
  # Após todas as tentativas falharem, a mensagem é enviada para a DLQ
  # e o offset é commitado para não travar o consumo.
  #
  # @param message [Kafka::FetchedMessage] a mensagem recebida
  # @return [void]
  def process_with_retry(message)
    attempt = 1

    loop do
      begin
        process_message(message, attempt)
        @consumer.mark_message_as_processed(message)
        puts '  ✅ Offset commitado no Kafka!'
        break
      rescue StandardError => e
        if attempt < MAX_RETRIES
          attempt += 1
          puts "  ⏳ Retry #{attempt - 1}/#{MAX_RETRIES} em 1 segundo..."
          sleep(1)
        else
          send_to_dlq(message, e)
          @consumer.mark_message_as_processed(message)
          puts '  ⚠️  Offset commitado (mensagem enviada para DLQ)'
          break
        end
      end
    end
  end

  # Publica a mensagem com falha no tópico de Dead Letter Queue.
  #
  # A mensagem original é encapsulada em um envelope com metadados
  # sobre a falha (motivo, tentativas, partição, offset original).
  #
  # @param original_message [Kafka::FetchedMessage] a mensagem que falhou
  # @param error [StandardError] a exceção que causou a falha
  # @return [void]
  def send_to_dlq(original_message, error)
    dlq_payload = build_dlq_payload(original_message, error)

    @dlq_producer.produce(dlq_payload, topic: DLQ_TOPIC, key: original_message.key)
    @dlq_producer.deliver_messages
    puts "  📦 Mensagem enviada para DLQ (#{DLQ_TOPIC})"
  end

  # Constrói o payload JSON para a mensagem de DLQ.
  #
  # @param msg [Kafka::FetchedMessage] a mensagem que falhou
  # @param error [StandardError] a exceção que causou a falha
  # @return [String] payload JSON com metadados da falha
  def build_dlq_payload(msg, error)
    build_dlq_envelope(msg, error).to_json
  end

  # Monta o hash com metadados da falha para envio à DLQ.
  #
  # @param msg [Kafka::FetchedMessage] a mensagem que falhou
  # @param error [StandardError] a exceção que causou a falha
  # @return [Hash] envelope com informações da mensagem original e do erro
  def build_dlq_envelope(msg, error)
    err = { class: error.class.name, message: error.message, backtrace: error.backtrace&.first(3) }

    { original_topic: msg.topic, original_partition: msg.partition,
      original_offset: msg.offset, original_key: msg.key,
      original_value: msg.value, error: err,
      failed_after_attempts: MAX_RETRIES, timestamp: Time.now.utc.iso8601 }
  end

  # Exibe os metadados e processa a mensagem, simulando erro aleatório.
  #
  # Simula um erro de processamento com 40% de chance para testar
  # o fluxo de retry e DLQ.
  #
  # @param message [Kafka::FetchedMessage] a mensagem recebida
  # @param attempt [Integer] número da tentativa atual
  # @return [void]
  def process_message(message, attempt)
    log_message_metadata(message, attempt)
    parse_payload(message.value)

    raise ProcessingError, 'Erro simulado no processamento da mensagem' if rand < 0.4

    puts '  ✅ Mensagem processada com sucesso!'
  end

  # Exibe os metadados da mensagem no terminal.
  #
  # @param message [Kafka::FetchedMessage] a mensagem recebida
  # @param attempt [Integer] número da tentativa atual
  # @return [void]
  def log_message_metadata(message, attempt)
    puts '------------------------'
    puts "Tentativa #{attempt}/#{MAX_RETRIES}"
    puts "Topic: #{message.topic}"
    puts "Partition: #{message.partition}"
    puts "Offset: #{message.offset}"
    puts "Key: #{message.key}"
    puts "Payload: #{message.value}"
  end

  # Tenta interpretar e exibir de forma organizada um conteúdo JSON.
  #
  # @param value [String] o conteúdo bruto da mensagem
  # @return [void]
  def parse_payload(value)
    data = JSON.parse(value)
    puts 'JSON:'
    p data
  rescue JSON::ParserError
    puts 'Mensagem não é JSON'
  end

  # Erro personalizado para simular falhas de processamento.
  class ProcessingError < StandardError; end
end
