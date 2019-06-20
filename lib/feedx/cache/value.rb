# A single value inside a cache.
class Feedx::Cache::Value
  attr_reader :key

  def initialize(cache, key)
    @cache = cache
    @key = key
  end

  # Read the key.
  def read(**opts)
    @cache.read(@key, **opts)
  end

  # Write a value.
  def write(value, **opts)
    @cache.write(@key, value, **opts)
  end

  # Fetches data. The optional block will be evaluated and the
  # result stored in the cache under the key in the event of a cache miss.
  def fetch(**opts, &block)
    @cache.fetch(@key, **opts, &block)
  end
end
