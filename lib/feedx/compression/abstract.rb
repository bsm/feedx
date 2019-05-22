class Feedx::Compression::Abstract
  def self.reader(_io, &_block)
    raise 'Not implemented'
  end

  def self.writer(_io, &_block)
    raise 'Not implemented'
  end
end
