require 'json'

class Feedx::Format::JSON < Feedx::Format::Abstract
  def decode(obj, **)
    line = @io.gets
    return unless line

    obj = obj.allocate if obj.is_a?(Class)
    obj.from_json(line)
    obj
  end

  def encode(msg, **opts)
    @io.write msg.to_json(**opts) << "\n"
  end
end
