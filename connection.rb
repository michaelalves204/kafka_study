# frozen_string_literal: true

require 'kafka'
require 'dotenv/load'

# Fornece uma conexão com um cluster Kafka.
#
# Lê as URLs dos brokers e um client_id a partir de variáveis de ambiente
# configuradas via +dotenv+, então instancia e retorna um novo cliente
# +Kafka+ pronto para ser usado por producers e consumers.
class Connection
  # Cria e retorna uma nova instância do cliente Kafka.
  #
  # @example
  #   Connection.new.create
  #
  # @return [Kafka::Client] um cliente Kafka instanciado e conectado
  #   aos brokers especificados na variável de ambiente +KAFKA_BROKERS+.
  def create
    Kafka.new(
      ENV['KAFKA_BROKERS'].split(','),
      client_id: ENV.fetch('CLIENT_ID', nil)
    )
  end
end
