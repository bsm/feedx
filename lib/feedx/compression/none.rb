class Feedx::Compression::None < Feedx::Compression::Abstract
  def self.reader(io, &block)
    block.call(io)
  end

  def self.writer(io, &block)
    block.call(io)
  end
end
