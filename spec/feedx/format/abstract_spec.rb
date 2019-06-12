require 'spec_helper'

RSpec.describe Feedx::Format::Abstract do
  subject   { Feedx::Format::JSON.new(wio) }
  let(:wio) { StringIO.new }

  it 'should decode each' do
    subject.encode(Feedx::TestCase::Model.new('X'))
    subject.encode(Feedx::TestCase::Model.new('Y'))
    subject.encode(Feedx::TestCase::Message.new(title: 'Z'))
    StringIO.open(wio.string) do |rio|
      fmt = subject.class.new(rio)
      dec = fmt.decode_each(Feedx::TestCase::Model).to_a
      expect(dec.map(&:title)).to eq(%w[X Y Z])
    end
  end
end
