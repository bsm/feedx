module Feedx
  module Format
    autoload :Abstract, 'feedx/format/abstract'
    autoload :JSON, 'feedx/format/json'
    autoload :Parquet, 'feedx/format/parquet'
    autoload :Protobuf, 'feedx/format/protobuf'

    class << self
      def validate!(kind)
        raise ArgumentError, "#{kind} does not implement #encoder(io, &block)" unless kind.respond_to?(:encoder)
        raise ArgumentError, "#{kind} does not implement #decoder(io, &block)" unless kind.respond_to?(:decoder)

        kind
      end

      def register(ext, kind)
        registry[ext.to_s] = validate!(kind)
      end

      def resolve(name)
        _resolve(name) || raise(ArgumentError, "invalid format #{name}")
      end

      def detect(path)
        base = File.basename(path)
        loop do
          ext = File.extname(base)
          raise ArgumentError, 'unable to detect format' if ext.empty?

          kind = _resolve(ext[1..]) || _resolve(ext[1..-2])
          return kind if kind

          base = base[0..-ext.size - 1]
        end
      end

      private

      def registry
        @registry ||= {
          'json'     => :JSON,
          'jsonl'    => :JSON,
          'ndjson'   => :JSON,
          'parquet'  => :Parquet,
          'pb'       => :Protobuf,
          'proto'    => :Protobuf,
          'protobuf' => :Protobuf,
        }
      end

      def _resolve(name)
        name = name.to_s
        kind = registry[name]
        if kind.is_a?(Symbol)
          kind = const_get(kind).new
          registry[name.to_s] = kind
        end
        kind
      end
    end
  end
end
