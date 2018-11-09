class Feedx::Format::Abstract
  def initialize(io)
    @io = io
  end

  def write(_msg)
    raise 'Not implemented'
  end
end
