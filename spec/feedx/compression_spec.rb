require 'spec_helper'

RSpec.describe Feedx::Compression do
  it 'should resolve' do
    expect(described_class.resolve(:gzip)).to eq(described_class::Gzip)
    expect(described_class.resolve(:gz)).to eq(described_class::Gzip)
    expect(described_class.resolve(nil)).to eq(described_class::None)
    expect { described_class.resolve(:txt) }.to raise_error(/invalid compression txt/)
  end

  it 'should detect' do
    expect(described_class.detect('path/to/file.jsonz')).to eq(described_class::Gzip)
    expect(described_class.detect('path/to/file.json.gz')).to eq(described_class::Gzip)
    expect(described_class.detect('path/to/file.json')).to eq(described_class::None)
    expect(described_class.detect('path/to/file.pbz')).to eq(described_class::Gzip)
    expect(described_class.detect('path/to/file.pb.gz')).to eq(described_class::Gzip)
    expect(described_class.detect('path/to/file.pb')).to eq(described_class::None)
  end
end
