class Feedx::Format::Abstract
  def decoder(io, **opts, &block)
    self.class::Decoder.open(io, **opts, &block)
  end

  def encoder(io, **opts, &block)
    self.class::Encoder.open(io, **opts, &block)
  end

  class Wrapper
    def self.open(io, **opts)
      inst = new(io, **opts)
      yield inst
    ensure
      inst&.close
    end

    def initialize(io, **)
      @io = io
    end
  end

  class Decoder < Wrapper
    def eof?
      @io.eof?
    end

    def decode_each(target, **opts)
      if block_given?
        yield decode(target, **opts) until eof?
      else
        Enumerator.new do |acc|
          acc << decode(target, **opts) until eof?
        end
      end
    end

    def decode(_target, **)
      raise 'Not implemented'
    end

    def close; end
  end

  class Encoder < Wrapper
    def encode(_msg, **)
      raise 'Not implemented'
    end

    def close
      @io.flush if @io.respond_to?(:flush)
    end
  end
end
