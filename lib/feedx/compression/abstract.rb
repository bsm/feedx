class Feedx::Compression::Abstract
  def reader(_io, **, &)
    raise 'Not implemented'
  end

  def writer(_io, **, &)
    raise 'Not implemented'
  end
end
