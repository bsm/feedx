require 'json'

class Feedx::Format::JSON < Feedx::Format::Abstract
  def write(msg)
    @io.write msg.to_json << "\n"
  end
end
