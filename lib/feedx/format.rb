module Feedx
  module Format
    autoload :Abstract, 'feedx/format/abstract'
    autoload :JSON, 'feedx/format/json'
    autoload :Protobuf, 'feedx/format/protobuf'

    class << self
      def register(ext, kind)
        raise ArgumentError, "#{kind} is not a subclass of Feedx::Format::Abstract" unless kind.is_a?(Class) && kind < Abstract

        registry[ext.to_s] = kind
      end

      def resolve(name)
        _resolve(name) || raise(ArgumentError, "invalid format #{name}")
      end

      def detect(path)
        base = File.basename(path)
        loop do
          ext = File.extname(base)
          raise ArgumentError, 'unable to detect format' if ext.empty?

          kind = _resolve(ext[1..-1]) || _resolve(ext[1..-2])
          return kind if kind

          base = base[0..-ext.size - 1]
        end
      end

      private

      def registry
        @registry ||= {
          'json'     => :JSON,
          'pb'       => :Protobuf,
          'proto'    => :Protobuf,
          'protobuf' => :Protobuf,
        }
      end

      def _resolve(name)
        name  = name.to_s
        klass = registry[name]
        if klass.is_a?(Symbol)
          klass = const_get(klass)
          registry[name.to_s] = klass
        end
        klass
      end
    end
  end
end
