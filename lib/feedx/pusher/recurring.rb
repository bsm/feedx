module Feedx
  class Pusher
    module Recurring
      DEFAULT_CHECK = proc do |enum|
        enum.respond_to?(:maximum) ? enum.maximum(:updated_at).to_f : 0.0
      end.freeze

      # Registered pushers
      def self.registry
        @registry ||= {}
      end

      # Performs a recurring Pusher task. See see Feedx::Pusher.new for additional
      # information on params and options.
      #
      # @param [String] name a unique task name.
      # @param [Hash] opts options.
      # @option [Proc] :check a custom revision check to evalute before perform.
      #   Receives the enum as an argument and returns a Numeric revision.
      #   Default: ->(enum) { enum.maximum(:updated_at).to_f }
      def self.perform(name, url, opts={}, &block)
        stored = registry[name]
        unless stored
          check  = opts.delete(:check) || DEFAULT_CHECK
          stored = { pusher: Feedx::Pusher.new(url, opts, &block), check: check, revision: 0 }
          registry[name] = stored
        end

        enum   = stored[:pusher].build_enum
        latest = stored[:check].call(enum).to_f
        return -1 unless stored[:revision] < latest

        size = stored[:pusher].perform(enum)
        stored[:revision] = latest
        size
      end
    end
  end
end
