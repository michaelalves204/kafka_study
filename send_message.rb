# frozen_string_literal: true

require_relative 'producer'

Producer.new.call('{"foo": "bars4"}', '1')
