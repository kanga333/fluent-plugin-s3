require 'arrow'
require 'parquet'
require 'json'
require 'msgpack'
require 'fluent/event'
require 'fluent/msgpack_factory'

module Fluent::Plugin
  class S3Output
    class ArrowCompressor < Compressor
      S3Output.register_compressor('arrow', self)

      config_param :format, :string, default: 'parquet'

      def configure(conf)
        super
      end

      def ext
        @format.freeze
      end

      def content_type
        'application/x-apache-arrow-file'.freeze
      end

      def compress(chunk, tmp)
        schema = ::Arrow::Schema.new([
          {"name" => "hello", "type" => "string"},
          {"name" => "empty", "type" => "int64"}
        ])

        # Create Arrow Batch
        pac = ::Fluent::MessagePackFactory.unpacker.feed(chunk.read)
        record_batch = ::Arrow::RecordBatch.new(schema, pac.each.to_a)
        # Save to file
        record_batch.to_table.save(tmp,
          format: @format,
          chunk_size: 1024)
      end
    end
  end
end
