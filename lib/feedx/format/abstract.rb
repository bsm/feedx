class Feedx::Format::Abstract
  def initialize(io)
    @io = io
  end

  def eof?
    @io.eof?
  end

  def decode_each(klass, **opts)
    if block_given?
      yield decode(klass, **opts) until eof?
    else
      Enumerator.new {|y| y << decode(klass, **opts) until eof? }
    end
  end

  def decode(_klass, **)
    raise 'Not implemented'
  end

  def encode(_msg, **)
    raise 'Not implemented'
  end
end
