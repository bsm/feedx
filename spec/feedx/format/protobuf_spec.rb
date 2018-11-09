require 'spec_helper'

RSpec.describe Feedx::Format::Protobuf do
  subject  { described_class.new(io) }
  let(:io) { StringIO.new }

  let(:model) do
    Class.new Struct.new(:title) do
      def to_pb
        Feedx::TestCase::Message.new title: title
      end
    end
  end

  it 'should write' do
    subject.write(model.new('X'))
    subject.write(model.new('Y'))
    expect(io.string.bytes).to eq([3, 10, 1, 88] + [3, 10, 1, 89])
  end
end
