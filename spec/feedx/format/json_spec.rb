require 'spec_helper'

RSpec.describe Feedx::Format::JSON do
  subject  { described_class.new(io) }
  let(:io) { StringIO.new }

  it 'should write' do
    subject.write(a: 1, b: 2)
    subject.write(c: ['x'], d: true)
    expect(io.string).to eq %({"a":1,"b":2}\n{"c":["x"],"d":true}\n)
  end
end
