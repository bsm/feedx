module Feedx
  module Compression
    autoload :Abstract, 'feedx/compression/abstract'
    autoload :None, 'feedx/compression/none'
    autoload :Gzip, 'feedx/compression/gzip'

    class << self
      def resolve(name)
        case name.to_s
        when 'gz', 'gzip'
          Gzip
        when ''
          None
        else
          raise ArgumentError, "invalid compression #{name}"
        end
      end

      def detect(path)
        if File.extname(path)[-1] == 'z'
          Gzip
        else
          None
        end
      end
    end
  end
end
