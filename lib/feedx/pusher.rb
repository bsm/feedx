require 'feedx'

module Feedx
  unless defined?(Gem::Deprecate) && Gem::Deprecate.skip
    warn "WARNING: Feedx::Pusher is deprecated; use Feedx::Producer instead (called from #{caller(2..2).first})."
  end
  Pusher = Producer
end
