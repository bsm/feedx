require 'uri'
require 'bfs'
require 'feedx'

module Feedx
  # Produces a relation as am encoded stream to a remote location.
  class Producer
    # See constructor.
    def self.perform(url, opts={}, &block)
      new(url, opts, &block).perform
    end

    # @param [String] url the destination URL.
    # @param [Hash] opts options
    # @option opts [Enumerable,ActiveRecord::Relation] :enum relation or enumerator to stream.
    # @option opts [Symbol,Class<Feedx::Format::Abstract>] :format custom formatter. Default: from file extension.
    # @option opts [Symbol,Class<Feedx::Compression::Abstract>] :compress enable compression. Default: from file extension.
    # @option opts [Time,Proc] :last_modified the last modified time, used to determine if a push is necessary.
    # @yield A block factory to generate the relation or enumerator.
    # @yieldreturn [Enumerable,ActiveRecord::Relation] the relation or enumerator to stream.
    def initialize(url, opts={}, &block)
      @enum = opts[:enum] || block
      raise ArgumentError, "#{self.class.name}.new expects an :enum option or a block factory" unless @enum

      @stream = Feedx::Stream.new(url, opts)
      @last_mod = opts[:last_modified]
    end

    def perform
      enum = @enum.is_a?(Proc) ? @enum.call : @enum
      last_mod = @last_mod.is_a?(Proc) ? @last_mod.call(enum) : @last_mod
      current  = (last_mod.to_f * 1000).floor

      begin
        previous = @stream.blob.info.metadata[META_LAST_MODIFIED].to_i
        return -1 unless current > previous
      rescue BFS::FileNotFound # rubocop:disable Lint/HandleExceptions
      end if current.positive?

      @stream.create metadata: { META_LAST_MODIFIED => current.to_s } do |fmt|
        iter = enum.respond_to?(:find_each) ? :find_each : :each
        enum.send(iter) {|rec| fmt.encode(rec) }
      end
      @stream.blob.info.size
    end
  end
end
