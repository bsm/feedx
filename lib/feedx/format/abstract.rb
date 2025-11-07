class Feedx::Format::Abstract
  def decoder(io, **, &)
    self.class::Decoder.open(io, **, &)
  end

  def encoder(io, **, &)
    self.class::Encoder.open(io, **, &)
  end

  class Wrapper
    def self.open(io, **)
      inst = new(io, **)
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
