require 'uri'
require 'bfs'

module Feedx
  # Pushes a relation as a protobuf encoded stream to an S3 location.
  class Pusher
    # @param [Enumerable,ActiveRecord::Relation] relation to stream.
    # @param [String] url the destination URL.
    # @param [Hash] opts options
    # @option opts [Symbol,Class<Feedx::Format::Abstract>] :format custom formatter. Default: from file extension.
    # @option opts [Symbol,Class<Feedx::Compression::Abstract>] :compress enable compression. Default: from file extension.
    def initialize(relation, url, opts={})
      @relation = relation
      @blob     = BFS::Blob.new(url)
      @format   = detect_format(opts[:format])
      @compress = detect_compress(opts[:compress])
    end

    def perform
      @blob.create do |io|
        @compress.wrap(io) {|w| write_to(w) }
      end
      @blob.info.size
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

    def write_to(io)
      stream = @format.new(io)
      enum = @relation.respond_to?(:find_each) ? :find_each : :each
      @relation.send(enum) {|rec| stream.write(rec) }
    end
  end
end
