# frozen_string_literal: true

require_relative 'consumer_retry'

# Consumidor que lê do tópico de Dead Letter Queue (DLQ).
#
# Útil para inspecionar mensagens que falharam após todas as tentativas
# de processamento no ConsumerRetry.
#
# Uso:
#   ruby run_dlq_consumer.rb
#
# O tópico DLQ é derivado automaticamente do TOPIC configurado no .env
# seguindo o padrão "<tópico-original>.dlq".

dlq_topic = "#{ENV.fetch('TOPIC')}.dlq"

kafka = Connection.new.create

consumer = kafka.consumer(
  group_id: 'dlq-inspector',
  offset_commit_interval: 10,
  offset_commit_threshold: 1
)

consumer.subscribe(dlq_topic)

puts "Lendo da Dead Letter Queue: #{dlq_topic}"
puts 'Aguardando mensagens...'
puts

consumer.each_message do |message|
  puts '=' * 50
  puts "Partition: #{message.partition} | Offset: #{message.offset}"
  puts "Key: #{message.key}"
  puts 'Payload:'
  puts message.value

  begin
    data = JSON.parse(message.value)
    puts
    puts 'Conteúdo interpretado:'
    puts "  Tópico original: #{data['original_topic']}"
    puts "  Partição original: #{data['original_partition']}"
    puts "  Offset original: #{data['original_offset']}"
    puts "  Erro: #{data.dig('error', 'class')}: #{data.dig('error', 'message')}"
    puts "  Falhou após: #{data['failed_after_attempts']} tentativa(s)"
    puts "  Timestamp: #{data['timestamp']}"
  rescue JSON::ParserError
    puts '  (conteúdo não é JSON)'
  end

  puts '=' * 50
  puts
end
