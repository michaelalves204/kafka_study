# frozen_string_literal: true

require 'kafka'
require_relative 'connection'
require 'dotenv/load'

# Producer envia mensagens para um tópico Kafka.
#
# @example Uso básico
#   Producer.new.call("hello world")
#
# Estabelece uma conexão via {Connection} e utiliza uma instância de producer
# para entregar mensagens ao tópico definido na variável de ambiente {TOPIC}.
class Producer
  # @return [String] o tópico Kafka lido da variável de ambiente +TOPIC+
  TOPIC = ENV.fetch('TOPIC', nil)

  # Instancia um novo producer Kafka através de {Connection}.
  #
  # @return [Producer] uma nova instância do producer
  def initialize
    @producer = Connection.new.create.producer
  end

  # Envia uma mensagem para o tópico Kafka configurado.
  #
  # @param message [String] o payload da mensagem a ser entregue
  # @return [void]
  def call(message, key = 'default')
    @producer.produce(
      message,
      topic: TOPIC,
      key: key
    )

    @producer.deliver_messages

    puts 'Mensagem enviada'
  end
end
