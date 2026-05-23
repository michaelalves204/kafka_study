# frozen_string_literal: true

require 'dotenv/load'
require 'kafka'
require 'json'
require_relative 'connection'

# Consumer lê mensagens de um tópico Kafka.
#
# @example Uso básico
#   Consumer.new(group_id: ENV.fetch('GROUP_ID')).call
#
# Estabelece uma conexão via {Connection} e se inscreve no tópico
# definido na variável de ambiente +TOPIC+. O ID do grupo de consumo
# é passado como parâmetro na inicialização.
class Consumer
  # @param group_id [String] o ID do grupo de consumo usado para
  #   coordenar o consumo de mensagens entre múltiplas instâncias.
  def initialize(group_id:)
    @consumer = Connection.new.create.consumer(
      group_id: group_id
    )

    @consumer.subscribe(ENV.fetch('TOPIC'))
  end

  # Inicia o loop de consumo de mensagens.
  #
  # Bloqueia indefinidamente, exibindo cada mensagem recebida no
  # terminal e tentando interpretar seu conteúdo como JSON.
  #
  # @return [void]
  def call
    puts 'Aguardando mensagens...'

    @consumer.each_message do |message|
      process_message(message)
    end
  rescue Interrupt
    puts "\nEncerrando consumer..."
    @consumer.stop
  end

  private

  # Exibe os metadados e o conteúdo bruto de uma mensagem Kafka.
  #
  # @param message [Kafka::FetchedMessage] a mensagem recebida do Kafka.
  # @return [void]
  def process_message(message)
    puts '------------------------'
    puts "Topic: #{message.topic}"
    puts "Partition: #{message.partition}"
    puts "Offset: #{message.offset}"
    puts "Key: #{message.key}"
    puts "Payload: #{message.value}"

    parse_payload(message.value)
  end

  # Tenta interpretar e exibir de forma organizada um conteúdo JSON.
  #
  # Caso o valor não seja um JSON válido, exibe uma mensagem amigável.
  #
  # @param value [String] o conteúdo bruto da mensagem.
  # @return [void]
  def parse_payload(value)
    data = JSON.parse(value)
    puts 'JSON:'
    p data
  rescue JSON::ParserError
    puts 'Mensagem não é JSON'
  end
end
