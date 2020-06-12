require 'bfs'
require 'feedx'

module Feedx
  # Abstract stream handler around a remote blob.
  class Stream
    attr_reader :blob

    # @param [String] url the blob URL.
    # @param [Hash] opts options
    # @option opts [Symbol,Class<Feedx::Format::Abstract>] :format custom formatter. Default: from file extension.
    # @option opts [Symbol,Class<Feedx::Compression::Abstract>] :compress enable compression. Default: from file extension.
    def initialize(url, **opts)
      @blob     = BFS::Blob.new(url)
      @format   = detect_format(opts[:format])
      @compress = detect_compress(opts[:compress])
    end

    # Opens the remote for reading.
    # @param [Hash] opts BFS::Blob#open options
    # @yield A block over a formatted stream.
    # @yieldparam [Feedx::Format::Abstract] formatted input stream.
    def open(**opts)
      @blob.open(**opts) do |io|
        @compress.reader(io) do |cio|
          @format.decoder(cio) do |fmt|
            yield fmt
          end
        end
      end
    end

    # Opens the remote for writing.
    # @param [Hash] opts BFS::Blob#create options
    # @yield A block over a formatted stream.
    # @yieldparam [Feedx::Format::Abstract] formatted output stream.
    def create(**opts)
      @blob.create(**opts) do |io|
        @compress.writer(io) do |cio|
          @format.encoder(cio) do |fmt|
            yield fmt
          end
        end
      end
    end

    private

    def detect_format(val)
      case val
      when nil
        Feedx::Format.detect(@blob.path)
      when String, Symbol
        Feedx::Format.resolve(val)
      else
        Feedx::Format.validate!(val)
      end
    end

    def detect_compress(val)
      case val
      when nil
        Feedx::Compression.detect(@blob.path)
      when String, Symbol
        Feedx::Compression.resolve(val)
      else
        Feedx::Compression.validate!(val)
      end
    end
  end
end
