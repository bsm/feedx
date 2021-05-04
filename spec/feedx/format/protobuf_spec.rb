require 'spec_helper'

RSpec.describe Feedx::Format::Protobuf do
  let(:wio) { StringIO.new }
  let(:rio) { StringIO.open(wio.string) }

  it 'encode/decodes' do
    subject.encoder wio do |enc|
      enc.encode(Feedx::TestCase::Model.new('X'))
      enc.encode(Feedx::TestCase::Model.new('Y'))
      enc.encode(Feedx::TestCase::Message.new(title: 'Z'))
    end
    expect(wio.string.bytes).to eq([3, 10, 1, 88] + [3, 10, 1, 89] + [3, 10, 1, 90])

    subject.decoder rio do |dec|
      expect(dec.decode(Feedx::TestCase::Message)).to eq(Feedx::TestCase::Message.new(title: 'X'))
      expect(dec.decode(Feedx::TestCase::Message)).to eq(Feedx::TestCase::Message.new(title: 'Y'))
      expect(dec.decode(Feedx::TestCase::Message)).to eq(Feedx::TestCase::Message.new(title: 'Z'))
      expect(dec.decode(Feedx::TestCase::Message)).to be_nil
      expect(dec).to be_eof
    end
  end
end
