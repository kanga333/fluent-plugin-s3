require 'arrow'
require 'parquet'
require 'json'
require 'fluent/event'
require 'fluent/msgpack_factory'

module Fluent::Plugin
  class S3Output
    class ArrowCompressor < Compressor
      S3Output.register_compressor('arrow', self)

      def configure(conf)
        super
      end

      def ext
        'arrow'.freeze
      end

      def content_type
        'application/x-apache-arrow-file'.freeze
      end

      def compress(chunk, tmp)
        schema = ::Arrow::Schema.new([{"name" => "hello", "type" => "string"}])
        # Create Arrow Batch
        msg = chunk.read.split(/\s+/).map {|val| JSON.parse(val)}
        record_batch = ::Arrow::RecordBatch.new(schema, msg)
        # Save to file
        record_batch.to_table.save(tmp,
          format: :parquet,
          chunk_size: 1024)
      end
    end
  end
end
