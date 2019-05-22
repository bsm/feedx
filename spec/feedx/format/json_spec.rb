require 'spec_helper'

RSpec.describe Feedx::Format::JSON do
  subject   { described_class.new(wio) }
  let(:wio) { StringIO.new }

  it 'should encode/decode' do
    subject.encode(Feedx::TestCase::Model.new('X'))
    subject.encode(Feedx::TestCase::Model.new('Y'))
    subject.encode(Feedx::TestCase::Message.new(title: 'Z'))
    expect(wio.string.lines).to eq [
      %({"title":"X","updated_at":"2018-01-05 11:25:15 UTC"}\n),
      %({"title":"Y","updated_at":"2018-01-05 11:25:15 UTC"}\n),
      %({"title":"Z"}\n),
    ]

    StringIO.open(wio.string) do |rio|
      fmt = described_class.new(rio)
      expect(fmt.decode(Feedx::TestCase::Model)).to eq(Feedx::TestCase::Model.new('X'))
      expect(fmt.decode(Feedx::TestCase::Model.new('O'))).to eq(Feedx::TestCase::Model.new('Y'))
      expect(fmt.decode(Feedx::TestCase::Model)).to eq(Feedx::TestCase::Model.new('Z'))
      expect(fmt.decode(Feedx::TestCase::Model)).to be_nil
      expect(fmt).to be_eof
    end
  end
end
