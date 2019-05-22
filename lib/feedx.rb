module Feedx
  META_LAST_MODIFIED = 'x-feedx-last-modified'.freeze

  autoload :Compression, 'feedx/compression'
  autoload :Format, 'feedx/format'
  autoload :Stream, 'feedx/stream'
  autoload :Producer, 'feedx/producer'
  autoload :Pusher, 'feedx/pusher'
  autoload :TaskState, 'feedx/task_state'
end
