class Feedx::Compression::None < Feedx::Compression::Abstract
  def self.wrap(io, &block)
    block.call(io)
  end
end
