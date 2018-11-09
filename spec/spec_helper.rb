require 'rspec'
require 'feedx'
require 'google/protobuf'

Google::Protobuf::DescriptorPool.generated_pool.build do
  add_message 'com.blacksquaremedia.feedx.testcase.Message' do
    optional :title, :string, 1
  end
end

module Feedx
  module TestCase
    Message = Google::Protobuf::DescriptorPool.generated_pool.lookup('com.blacksquaremedia.feedx.testcase.Message').msgclass
  end
end
