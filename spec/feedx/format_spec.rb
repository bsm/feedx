require 'spec_helper'

RSpec.describe Feedx::Format do
  it 'should resolve' do
    expect(described_class.resolve(:json)).to eq(described_class::JSON)
    expect(described_class.resolve(:pb)).to eq(described_class::Protobuf)
    expect { described_class.resolve(:txt) }.to raise_error(/invalid format txt/)
  end

  it 'should detect' do
    expect(described_class.detect('path/to/file.json')).to eq(described_class::JSON)
    expect(described_class.detect('path/to/file.jsonz')).to eq(described_class::JSON)
    expect(described_class.detect('path/to/file.json.gz')).to eq(described_class::JSON)
    expect(described_class.detect('path/to/file.pb')).to eq(described_class::Protobuf)
    expect(described_class.detect('path/to/file.pbz')).to eq(described_class::Protobuf)
    expect(described_class.detect('path/to/file.pb.z')).to eq(described_class::Protobuf)
    expect do
      described_class.detect('path/to/file.txt')
    end.to raise_error(/unable to detect format/)
  end
end
