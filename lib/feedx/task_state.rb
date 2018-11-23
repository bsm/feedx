require 'delegate'

module Feedx
  # Maintains the state of global tasks.
  module TaskState
    class << self

      # Perform something with block.
      # @param [String] name the task name.
      # @option [Hash] default the default state to return (if none exists).
      # @yield Evaluates the block with state.
      # @yieldparam [Hash] state the state of the registered task
      def with(name, default: {})
        default = {} unless default.is_a?(Hash)
        yield(self[name] ||= default)
      end

      # Clears the registry.
      def clear
        _meta.clear
      end

      # @param [String] name the task name.
      def [](name)
        _meta[name]
      end

      # @param [String] name the name of the task.
      # @param [Hash] the state to use.
      def []=(name, state)
        _meta[name] = state if state.is_a?(Hash)
      end

      protected

      def _meta
        @_meta ||= {} # rubocop:disable Naming/MemoizedInstanceVariableName
      end
    end
  end
end
