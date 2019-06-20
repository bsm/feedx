require 'monitor'

# Thread-safe in-memory cache. Use for testing only.
class Feedx::Cache::Memory < Feedx::Cache::Abstract
  def initialize
    @monitor = Monitor.new
    @entries = {}
  end

  # Clear empties cache.
  def clear
    @monitor.synchronize do
      @entries.clear
    end
  end

  # Read reads a key.
  def read(key, **)
    @monitor.synchronize do
      @entries[key]
    end
  end

  # Write writes a key.
  def write(key, value, **)
    @monitor.synchronize do
      @entries[key] = value.to_s
    end
  end
end
