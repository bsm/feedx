module Feedx
  module Compression
    autoload :Abstract, 'feedx/compression/abstract'
    autoload :None, 'feedx/compression/none'
    autoload :Gzip, 'feedx/compression/gzip'

    class << self
      def validate!(kind)
        raise ArgumentError, "#{kind} does not implement #reader(io, &block)" unless kind.respond_to?(:reader)
        raise ArgumentError, "#{kind} does not implement #writer(io, &block)" unless kind.respond_to?(:writer)

        kind
      end

      def resolve(name)
        case name.to_s
        when 'gz', 'gzip'
          Gzip.new
        when ''
          None.new
        else
          raise ArgumentError, "invalid compression #{name}"
        end
      end

      def detect(path)
        if File.extname(path)[-1] == 'z'
          Gzip.new
        else
          None.new
        end
      end
    end
  end
end
