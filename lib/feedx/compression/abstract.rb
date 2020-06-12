class Feedx::Compression::Abstract
  def reader(_io, &_block)
    raise 'Not implemented'
  end

  def writer(_io, &_block)
    raise 'Not implemented'
  end
end
