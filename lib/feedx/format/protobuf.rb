require 'pbio'

class Feedx::Format::Protobuf < Feedx::Format::Abstract
  def initialize(io)
    super PBIO::Delimited.new(io)
  end

  def decode(klass)
    @io.read(klass)
  end

  def encode(msg)
    msg = msg.to_pb if msg.respond_to?(:to_pb)
    @io.write msg
  end
end
