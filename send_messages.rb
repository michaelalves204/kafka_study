# frozen_string_literal: true

require_relative 'producer'

# Envia múltiplas mensagens para testar o fluxo de retry e DLQ.
#
# Uso:
#   ruby send_messages.rb
#
# Publica 5 mensagens sequenciais no tópico configurado.

producer = Producer.new

messages = [
  { id: 1, text: 'Primeira mensagem', user: 'alice' },
  { id: 2, text: 'Segunda mensagem', user: 'bob' },
  { id: 3, text: 'Terceira mensagem', user: 'charlie' },
  { id: 4, text: 'Quarta mensagem', user: 'diana' },
  { id: 5, text: 'Quinta mensagem', user: 'eve' }
]

puts "Publicando #{messages.size} mensagens no tópico #{Producer::TOPIC}..."
puts

messages.each do |msg|
  payload = msg.to_json
  puts "  [#{msg[:id]}/#{messages.size}] Enviando: #{payload}"
  producer.call(payload)
  sleep(0.5)
end

puts
puts 'Todas as mensagens foram enviadas!'
