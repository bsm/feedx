require 'bfs'
require 'feedx'

module Feedx
  # Abstract stream handler around a remote blob.
  class Stream
    attr_reader :blob

    # Behaves like new, but accepts an optional block.
    # If a block is given, streams are automatically closed after the block is yielded.
    def self.open(url, **opts)
      stream = new(url, **opts)
      return stream unless block_given?

      begin
        yield stream
      ensure
        stream.close
      end
    end

    # @param [String] url the blob URL.
    # @param [Hash] opts options
    # @option opts [Symbol,Class<Feedx::Format::Abstract>] :format custom formatter. Default: from file extension.
    # @option opts [Symbol,Class<Feedx::Compression::Abstract>] :compress enable compression. Default: from file extension.
    def initialize(url, format: nil, compress: nil, **opts)
      @blob     = BFS::Blob.new(url)
      @format   = detect_format(format)
      @compress = detect_compress(compress)
      @opts     = opts

      BFS.defer(self, :close)
    end

    # Opens the remote for reading.
    # @param [Hash] opts BFS::Blob#open options
    # @yield A block over a formatted stream.
    # @yieldparam [Feedx::Format::Abstract] formatted input stream.
    def open(**opts)
      @blob.open(**opts) do |io|
        @compress.reader(io, **@opts) do |cio|
          @format.decoder(cio, **@opts) do |fmt|
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
        @compress.writer(io, **@opts) do |cio|
          @format.encoder(cio, **@opts) do |fmt|
            yield fmt
          end
        end
      end
    end

    # Closes the underlying connection.
    def close
      @blob.close
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
