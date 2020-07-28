require 'arrow'
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
        # Unpack chunk as msgpack
        pac = ::Fluent::MessagePackFactory.unpacker.feed(chunk)
        # Create Arrow Batch
        record_batch = ::Arrow::RecordBatch.new(schema, pac)
        # Save to file
        record_batch.to_table.save(tmp,
          format: :arrow,
          chunk_size: 1024)
      end
    end
  end
end
