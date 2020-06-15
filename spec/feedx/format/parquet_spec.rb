require 'spec_helper'

RSpec.describe Feedx::Format::Parquet do
  let(:wio) { StringIO.new }
  let(:rio) { StringIO.open(wio.string) }

  let(:schema) do
    Arrow::Schema.new([
      Arrow::Field.new('title', :string),
      Arrow::Field.new('updated_at', type: :timestamp, unit: :second),
    ])
  end

  it 'should encode/decode' do
    subject.encoder wio, schema: schema, batch_size: 2 do |enc|
      enc.encode(Feedx::TestCase::Model.new('X'))
      enc.encode(Feedx::TestCase::Model.new('Y'))
      enc.encode(Feedx::TestCase::Model.new('Z'))
    end
    expect(wio.string.bytesize).to be_within(100).of(1100)

    subject.decoder rio do |dec|
      expect(dec.decode(Feedx::TestCase::Model)).to eq(Feedx::TestCase::Model.new('X'))
      expect(dec.decode(Feedx::TestCase::Model)).to eq(Feedx::TestCase::Model.new('Y'))
      expect(dec.decode(Feedx::TestCase::Model)).to eq(Feedx::TestCase::Model.new('Z'))
      expect(dec.decode(Feedx::TestCase::Model)).to be_nil
      expect(dec).to be_eof
    end
  end
end
