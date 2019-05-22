class Feedx::Format::Abstract
  def initialize(io)
    @io = io
  end

  def eof?
    @io.eof?
  end

  def decode(_klass)
    raise 'Not implemented'
  end

  def encode(_msg)
    raise 'Not implemented'
  end
end
