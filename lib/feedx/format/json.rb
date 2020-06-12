require 'json'

class Feedx::Format::JSON < Feedx::Format::Abstract
  class Decoder < Feedx::Format::Abstract::Decoder
    def decode(target, **)
      line = @io.gets
      return unless line

      target = target.allocate if target.is_a?(Class)
      target.from_json(line)
      target
    end
  end

  class Encoder < Feedx::Format::Abstract::Encoder
    def encode(msg, **opts)
      @io.write msg.to_json(**opts) << "\n"
    end
  end
end
