module Feedx
  META_VERSION = 'X-Feedx-Version'.freeze
  META_VERSION_DC = META_VERSION.downcase.freeze

  autoload :Cache, 'feedx/cache'
  autoload :Compression, 'feedx/compression'
  autoload :Consumer, 'feedx/consumer'
  autoload :Format, 'feedx/format'
  autoload :Stream, 'feedx/stream'
  autoload :Producer, 'feedx/producer'
  autoload :Pusher, 'feedx/pusher'
end
