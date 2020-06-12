class Feedx::Compression::None < Feedx::Compression::Abstract
  def reader(io)
    yield(io)
  end

  def writer(io)
    yield(io)
  end
end
