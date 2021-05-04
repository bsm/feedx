require 'spec_helper'

RSpec.describe Feedx::Format::JSON do
  let(:wio) { StringIO.new }
  let(:rio) { StringIO.open(wio.string) }

  it 'encode/decodes' do
    subject.encoder wio do |enc|
      enc.encode(Feedx::TestCase::Model.new('X'))
      enc.encode(Feedx::TestCase::Model.new('Y'))
      enc.encode(Feedx::TestCase::Message.new(title: 'Z'))
    end
    expect(wio.string).to eq(<<~JSON)
      {"title":"X","updated_at":"2018-01-05 11:25:15 UTC"}
      {"title":"Y","updated_at":"2018-01-05 11:25:15 UTC"}
      {"title":"Z"}
    JSON

    subject.decoder rio do |dec|
      expect(dec.decode(Feedx::TestCase::Model)).to eq(Feedx::TestCase::Model.new('X'))
      expect(dec.decode(Feedx::TestCase::Model.new('O'))).to eq(Feedx::TestCase::Model.new('Y'))
      expect(dec.decode(Feedx::TestCase::Model)).to eq(Feedx::TestCase::Model.new('Z'))
      expect(dec.decode(Feedx::TestCase::Model)).to be_nil
      expect(dec).to be_eof
    end
  end
end
