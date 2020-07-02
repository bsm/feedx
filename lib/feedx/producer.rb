require 'uri'
require 'bfs'
require 'feedx'

module Feedx
  # Produces a relation as an encoded feed to a remote location.
  class Producer
    # See constructor.
    def self.perform(url, **opts, &block)
      new(url, **opts, &block).perform
    end

    # @param [String] url the destination URL.
    # @param [Hash] opts options
    # @option opts [Enumerable,ActiveRecord::Relation] :enum relation or enumerator to stream.
    # @option opts [Symbol,Class<Feedx::Format::Abstract>] :format custom formatter. Default: from file extension.
    # @option opts [Symbol,Class<Feedx::Compression::Abstract>] :compress enable compression. Default: from file extension.
    # @option opts [Time,Proc] :last_modified the last modified time, used to determine if a push is necessary.
    # @yield A block factory to generate the relation or enumerator.
    # @yieldreturn [Enumerable,ActiveRecord::Relation] the relation or enumerator to stream.
    def initialize(url, last_modified: nil, format_options: {}, enum: nil, **opts, &block)
      @enum = enum || block
      raise ArgumentError, "#{self.class.name}.new expects an :enum option or a block factory" unless @enum

      @url  = url
      @opts = opts.merge(format_options)
      @last_mod = last_modified

      return if format_options.empty? || (defined?(Gem::Deprecate) && Gem::Deprecate.skip)

      warn "WARNING: passing format_options is deprecated; pass the options inline instead (called from #{caller(2..2).first})."
    end

    def perform
      stream = Feedx::Stream.new(@url, **@opts)
      enum = @enum.is_a?(Proc) ? @enum.call : @enum
      last_mod = @last_mod.is_a?(Proc) ? @last_mod.call(enum) : @last_mod
      local_rev = last_mod.is_a?(Integer) ? last_mod : (last_mod.to_f * 1000).floor

      begin
        metadata   = stream.blob.info.metadata
        remote_rev = (metadata[META_LAST_MODIFIED] || metadata[META_LAST_MODIFIED_DC]).to_i
        return -1 unless local_rev > remote_rev
      rescue BFS::FileNotFound
        nil
      end if local_rev.positive?

      stream.create metadata: { META_LAST_MODIFIED => local_rev.to_s } do |fmt|
        iter = enum.respond_to?(:find_each) ? :find_each : :each
        enum.send(iter) {|rec| fmt.encode(rec, **@opts) }
      end
      stream.blob.info.size
    ensure
      stream&.close
    end
  end
end
