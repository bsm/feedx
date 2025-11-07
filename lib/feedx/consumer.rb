require 'uri'
require 'bfs'
require 'feedx'

module Feedx
  # Consumes an enumerates over a feed.
  class Consumer
    include Enumerable

    # See constructor.
    def self.each(url, klass, **, &)
      new(url, klass, **).each(&)
    end

    # @param [String] url the destination URL.
    # @param [Class] klass the record class.
    # @param [Hash] opts options
    # @option opts [Symbol,Class<Feedx::Format::Abstract>] :format custom formatter. Default: from file extension.
    # @option opts [Symbol,Class<Feedx::Compression::Abstract>] :compress enable compression. Default: from file extension.
    # @option opts [Feedx::Cache::Value] :cache cache value to store remote version and consume conditionally.
    def initialize(url, klass, format_options: {}, cache: nil, **opts)
      @klass = klass
      @url = url
      @opts = opts.merge(format_options)
      @cache = cache

      return if format_options.empty? || (defined?(Gem::Deprecate) && Gem::Deprecate.skip)

      warn "WARNING: passing format_options is deprecated; pass the options inline instead (called from #{caller(2..2).first})."
    end

    # @return [Boolean] returns true if performed.
    def each(&block)
      stream = Feedx::Stream.new(@url, **@opts)
      remote_ver = nil

      if @cache
        metadata   = stream.blob.info.metadata
        local_ver  = @cache.read.to_i
        remote_ver = (metadata[META_VERSION] || metadata[META_VERSION_DC]).to_i
        return false if remote_ver.positive? && remote_ver <= local_ver
      end

      stream.open do |fmt|
        fmt.decode_each(@klass, **@opts, &block)
      end
      @cache.write(remote_ver) if @cache && remote_ver

      true
    ensure
      stream&.close
    end
  end
end
