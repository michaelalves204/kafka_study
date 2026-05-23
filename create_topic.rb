# frozen_string_literal: true

require 'kafka'
require_relative 'connection'

Connection.new.create.create_topic(
  ENV.fetch('TOPIC', nil),
  num_partitions: 3,
  replication_factor: 2
)
