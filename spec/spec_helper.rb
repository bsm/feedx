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

    class Model
      attr_reader :title

      def initialize(title)
        @title = title
      end

      def to_pb(*)
        Feedx::TestCase::Message.new title: @title
      end

      def ==(other)
        title == other.title
      end
      alias eql? ==

      def from_json(data, *)
        hash = ::JSON.parse(data)
        @title = hash['title'] if hash.is_a?(Hash)
      end

      def to_json(*)
        ::JSON.dump(title: @title, updated_at: Time.at(1515151515).utc)
      end
    end
  end
end
