require 'spec_helper'

RSpec.describe Feedx::Compression::None do
  it 'should wrap' do
    io = StringIO.new
    described_class.wrap(io) {|w| w.write 'xyz' * 1000 }
    expect(io.size).to eq(3000)
  end
end
