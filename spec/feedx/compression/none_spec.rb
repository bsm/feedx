require 'spec_helper'

RSpec.describe Feedx::Compression::None do
  it 'should wrap readers/writers' do
    wio = StringIO.new
    subject.writer(wio) {|w| w.write 'xyz' * 1000 }
    expect(wio.size).to eq(3000)

    data = ''
    StringIO.open(wio.string) do |rio|
      subject.reader(rio) {|z| data = z.read }
    end
    expect(data.size).to eq(3000)
  end
end
