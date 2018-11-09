require 'pbio'

class Feedx::Format::Protobuf < Feedx::Format::Abstract
  def initialize(io)
    super PBIO::Delimited.new(io)
  end

  def write(msg)
    @io.write msg.to_pb
  end
end
