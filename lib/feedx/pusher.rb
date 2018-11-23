require 'uri'
require 'bfs'

module Feedx
  # Pushes a relation as a protobuf encoded stream to an S3 location.
  class Pusher
    autoload :Recurring, 'feedx/pusher/recurring'

    # See constructor.
    def self.perform(url, opts={}, &block)
      new(url, opts, &block).perform
    end

    # @param [String] url the destination URL.
    # @param [Hash] opts options
    # @option opts [Enumerable,ActiveRecord::Relation] :enum relation or enumerator to stream.
    # @option opts [Symbol,Class<Feedx::Format::Abstract>] :format custom formatter. Default: from file extension.
    # @option opts [Symbol,Class<Feedx::Compression::Abstract>] :compress enable compression. Default: from file extension.
    # @yield A block factory to generate the relation or enumerator.
    # @yieldreturn [Enumerable,ActiveRecord::Relation] the relation or enumerator to stream.
    def initialize(url, opts={}, &block)
      @enum = opts[:enum] || block
      raise ArgumentError, "#{self.class.name}.new expects an :enum option or a block factory" unless @enum

      @blob     = BFS::Blob.new(url)
      @format   = detect_format(opts[:format])
      @compress = detect_compress(opts[:compress])
    end

    def perform(enum=self.enum)
      @blob.create do |io|
        @compress.wrap(io) {|w| write_enum(enum, w) }
      end
      @blob.info.size
    end

    # @return [Enumerable,ActiveRecord::Relation] the relation or enumerator.
    def enum
      @enum.is_a?(Proc) ? @enum.call : @enum
    end

    private

    def detect_format(val)
      case val
      when nil
        Feedx::Format.detect(@blob.path)
      when Class
        parent = Feedx::Format::Abstract
        raise ArgumentError, "Class #{val} must extend #{parent}" unless val < parent

        val
      else
        Feedx::Format.resolve(val)
      end
    end

    def detect_compress(val)
      case val
      when nil
        Feedx::Compression.detect(@blob.path)
      when Class
        parent = Feedx::Compression::Abstract
        raise ArgumentError, "Class #{val} must extend #{parent}" unless val < parent

        val
      else
        Feedx::Compression.resolve(val)
      end
    end

    def write_enum(enum, io)
      stream   = @format.new(io)
      iterator = enum.respond_to?(:find_each) ? :find_each : :each
      enum.send(iterator) {|rec| stream.write(rec) }
    end
  end
end
