# A single value inside a cache.
class Feedx::Cache::Value
  attr_reader :key

  def initialize(cache, key)
    @cache = cache
    @key = key
  end

  # Read the key.
  def read(**)
    @cache.read(@key, **)
  end

  # Write a value.
  def write(value, **)
    @cache.write(@key, value, **)
  end

  # Fetches data. The optional block will be evaluated and the
  # result stored in the cache under the key in the event of a cache miss.
  def fetch(**, &)
    @cache.fetch(@key, **, &)
  end
end
