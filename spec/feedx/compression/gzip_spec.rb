require 'spec_helper'

RSpec.describe Feedx::Compression::Gzip do
  it 'should wrap' do
    io = StringIO.new
    described_class.wrap(io) {|w| w.write 'xyz' * 1000 }
    expect(io.size).to be_within(20).of(40)
  end
end
