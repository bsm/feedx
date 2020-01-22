require 'spec_helper'

RSpec.describe Feedx::Compression::Gzip do
  it 'should wrap readers/writers' do
    wio = StringIO.new
    described_class.writer(wio) {|w| w.write 'xyz' * 1000 }
    expect(wio.size).to be_within(20).of(40)
    expect(wio.string.encoding).to eq(Encoding::BINARY)

    data = ''
    StringIO.open(wio.string) do |rio|
      described_class.reader(rio) {|z| data = z.read }
    end
    expect(data.size).to eq(3000)
    expect(data.encoding).to eq(Encoding.default_external)
  end
end
