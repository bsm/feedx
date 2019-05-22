require 'spec_helper'

RSpec.describe Feedx::Format::Protobuf do
  subject   { described_class.new(wio) }
  let(:wio) { StringIO.new }

  it 'should encode/decode' do
    subject.encode(Feedx::TestCase::Model.new('X'))
    subject.encode(Feedx::TestCase::Model.new('Y'))
    subject.encode(Feedx::TestCase::Message.new(title: 'Z'))
    expect(wio.string.bytes).to eq([3, 10, 1, 88] + [3, 10, 1, 89] + [3, 10, 1, 90])

    StringIO.open(wio.string) do |rio|
      fmt = described_class.new(rio)
      expect(fmt.decode(Feedx::TestCase::Message)).to eq(Feedx::TestCase::Message.new(title: 'X'))
      expect(fmt.decode(Feedx::TestCase::Message)).to eq(Feedx::TestCase::Message.new(title: 'Y'))
      expect(fmt.decode(Feedx::TestCase::Message)).to eq(Feedx::TestCase::Message.new(title: 'Z'))
      expect(fmt.decode(Feedx::TestCase::Message)).to be_nil
      expect(fmt).to be_eof
    end
  end
end
