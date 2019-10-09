module Feedx
  META_LAST_MODIFIED = 'X-Feedx-Last-Modified'.freeze
  META_LAST_MODIFIED_DC = META_LAST_MODIFIED.downcase.freeze

  autoload :Cache, 'feedx/cache'
  autoload :Compression, 'feedx/compression'
  autoload :Consumer, 'feedx/consumer'
  autoload :Format, 'feedx/format'
  autoload :Stream, 'feedx/stream'
  autoload :Producer, 'feedx/producer'
  autoload :Pusher, 'feedx/pusher'
  autoload :TaskState, 'feedx/task_state'
end
