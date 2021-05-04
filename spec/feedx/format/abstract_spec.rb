require 'spec_helper'

RSpec.describe Feedx::Format::Abstract do
  subject   { Feedx::Format::JSON.new }

  let(:wio) { StringIO.new }
  let(:rio) { StringIO.open(wio.string) }

  it 'decodes each' do
    subject.encoder wio do |enc|
      enc.encode(Feedx::TestCase::Model.new('X'))
      enc.encode(Feedx::TestCase::Model.new('Y'))
      enc.encode(Feedx::TestCase::Message.new(title: 'Z'))
    end

    subject.decoder rio do |dec|
      acc = dec.decode_each(Feedx::TestCase::Model).to_a
      expect(acc.map(&:title)).to eq(%w[X Y Z])
    end
  end
end
