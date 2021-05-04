require 'spec_helper'

RSpec.describe Feedx::Format do
  it 'resolves' do
    expect(described_class.resolve(:json)).to be_instance_of(described_class::JSON)
    expect(described_class.resolve(:pb)).to be_instance_of(described_class::Protobuf)
    expect { described_class.resolve(:txt) }.to raise_error(/invalid format txt/)
  end

  it 'detects' do
    expect(described_class.detect('path/to/file.json')).to be_instance_of(described_class::JSON)
    expect(described_class.detect('path/to/file.jsonz')).to be_instance_of(described_class::JSON)
    expect(described_class.detect('path/to/file.json.gz')).to be_instance_of(described_class::JSON)
    expect(described_class.detect('path/to/file.pb')).to be_instance_of(described_class::Protobuf)
    expect(described_class.detect('path/to/file.pbz')).to be_instance_of(described_class::Protobuf)
    expect(described_class.detect('path/to/file.pb.z')).to be_instance_of(described_class::Protobuf)
    expect do
      described_class.detect('path/to/file.txt')
    end.to raise_error(/unable to detect format/)
  end
end
