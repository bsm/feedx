require 'uri'
require 'bfs'
require 'feedx'

module Feedx
  # Consumes an enumerates over a feed.
  class Consumer
    include Enumerable

    # See constructor.
    def self.each(url, klass, opts = {}, &block)
      new(url, klass, opts).each(&block)
    end

    # @param [String] url the destination URL.
    # @param [Class] klass the record class.
    # @param [Hash] opts options
    # @option opts [Symbol,Class<Feedx::Format::Abstract>] :format custom formatter. Default: from file extension.
    # @option opts [Hash] :format_options format decode options. Default: {}.
    # @option opts [Symbol,Class<Feedx::Compression::Abstract>] :compress enable compression. Default: from file extension.
    # @option opts [Feedx::Cache::Value] :cache cache value to store remote last modified time and consume conditionally.
    def initialize(url, klass, opts = {})
      @klass    = klass
      @stream   = Feedx::Stream.new(url, opts)
      @fmt_opts = opts[:format_options] || {}
      @cache    = opts[:cache]
    end

    # @return [Boolean] returns true if performed.
    def each(&block)
      remote_rev = nil

      if @cache
        metadata   = @stream.blob.info.metadata
        local_rev  = @cache.read.to_i
        remote_rev = (metadata[META_LAST_MODIFIED] || metadata[META_LAST_MODIFIED_DC]).to_i
        return false if remote_rev.positive? && remote_rev <= local_rev
      end

      @stream.open do |fmt|
        fmt.decode_each(@klass, **@fmt_opts, &block)
      end
      @cache.write(remote_rev) if @cache && remote_rev

      true
    end
  end
end
