module Feedx
  META_LAST_MODIFIED = 'x-feedx-last-modified'.freeze

  autoload :Cache, 'feedx/cache'
  autoload :Compression, 'feedx/compression'
  autoload :Consumer, 'feedx/consumer'
  autoload :Format, 'feedx/format'
  autoload :Stream, 'feedx/stream'
  autoload :Producer, 'feedx/producer'
  autoload :Pusher, 'feedx/pusher'
  autoload :TaskState, 'feedx/task_state'
end
