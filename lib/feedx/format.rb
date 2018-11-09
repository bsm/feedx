module Feedx
  module Format
    autoload :Abstract, 'feedx/format/abstract'
    autoload :JSON, 'feedx/format/json'
    autoload :Protobuf, 'feedx/format/protobuf'

    class << self
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

      def _resolve(name)
        case name.to_s
        when 'pb', 'proto', 'protobuf'
          Protobuf
        when 'json'
          JSON
        end
      end
    end
  end
end
