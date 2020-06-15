require 'pbio'

class Feedx::Format::Protobuf < Feedx::Format::Abstract
  class Decoder < Feedx::Format::Abstract::Decoder
    def initialize(io, **opts)
      super PBIO::Delimited.new(io), **opts
    end

    def decode(target, **)
      @io.read(target)
    end
  end

  class Encoder < Feedx::Format::Abstract::Encoder
    def initialize(io, **opts)
      super PBIO::Delimited.new(io), **opts
    end

    def encode(msg, **opts)
      msg = msg.to_pb(**opts) if msg.respond_to?(:to_pb)
      @io.write msg
    end
  end
end
