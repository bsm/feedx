class Feedx::Cache::Abstract
  # Clears cache.
  def clear
    raise 'Not implemented'
  end

  # Read reads a key.
  def read(_key, **)
    raise 'Not implemented'
  end

  # Write writes a key/value pair.
  def write(_key, _value, **)
    raise 'Not implemented'
  end

  # Fetches data from the cache, using the given key.
  # The optional block will be evaluated and the result stored in the cache
  # in the event of a cache miss.
  def fetch(key, **)
    value = read(key, **)

    if block_given?
      value ||= yield
      write(key, value, **) if value
    end

    value
  end

  # @return [Feedx::Abstract::Value] returns a wrapper around a single value.
  def value(key)
    Feedx::Cache::Value.new(self, key)
  end
end
