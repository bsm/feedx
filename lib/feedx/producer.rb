require 'uri'
require 'bfs'
require 'feedx'

module Feedx
  # Produces a relation as an encoded feed to a remote location.
  class Producer
    # See constructor.
    def self.perform(url, **, &)
      new(url, **, &).perform
    end

    # @param [String] url the destination URL.
    # @param [Hash] opts options
    # @option opts [Enumerable,ActiveRecord::Relation] :enum relation or enumerator to stream.
    # @option opts [Symbol,Class<Feedx::Format::Abstract>] :format custom formatter. Default: from file extension.
    # @option opts [Symbol,Class<Feedx::Compression::Abstract>] :compress enable compression. Default: from file extension.
    # @option opts [Integer] :version the most recent version, used to determine if a push is necessary.
    # @yield A block factory to generate the relation or enumerator.
    # @yieldreturn [Enumerable,ActiveRecord::Relation] the relation or enumerator to stream.
    def initialize(url, version: nil, format_options: {}, enum: nil, **opts, &block)
      @enum = enum || block
      raise ArgumentError, "#{self.class.name}.new expects an :enum option or a block factory" unless @enum

      @url  = url
      @opts = opts.merge(format_options)
      @version = version

      return if format_options.empty? || (defined?(Gem::Deprecate) && Gem::Deprecate.skip)

      warn "WARNING: passing format_options is deprecated; pass the options inline instead (called from #{caller(2..2).first})."
    end

    def perform
      Feedx::Stream.open(@url, **@opts) do |stream|
        enum = @enum.is_a?(Proc) ? @enum.call : @enum
        local_ver = @version.is_a?(Proc) ? @version.call(enum) : @version
        local_ver = local_ver.to_i

        begin
          metadata   = stream.blob.info.metadata
          remote_ver = (metadata[META_VERSION] || metadata[META_VERSION_DC]).to_i
          return -1 unless local_ver > remote_ver
        rescue BFS::FileNotFound
          nil
        end if local_ver.positive?

        stream.create metadata: { META_VERSION => local_ver.to_s } do |fmt|
          iter = enum.respond_to?(:find_each) ? :find_each : :each
          enum.send(iter) {|rec| fmt.encode(rec, **@opts) }
        end
        stream.blob.info.size
      end
    end
  end
end
