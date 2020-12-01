require 'spec_helper'

RSpec.describe Feedx::Consumer do
  let(:bucket) { BFS::Bucket::InMem.new }
  let(:klass)  { Feedx::TestCase::Model }
  let(:cache)  { Feedx::Cache::Memory.new.value('my-consumer') }
  before { allow(BFS).to receive(:resolve).and_return(bucket) }

  it 'should reject invalid inputs' do
    expect do
      described_class.each('mock:///dir/file.txt', klass)
    end.to raise_error(/unable to detect format/)
  end

  it 'should consume feeds' do
    url = mock_produce!
    csm = described_class.new(url, klass)
    expect(csm).to be_a(Enumerable)

    cnt = csm.count do |rec|
      expect(rec).to be_instance_of(klass)
      true
    end
    expect(cnt).to eq(300)
  end

  it 'should perform conditionally' do
    url = mock_produce! last_modified: Time.at(1515151515)
    expect(described_class.new(url, klass, cache: cache).count).to eq(300)
    expect(described_class.new(url, klass, cache: cache).count).to eq(0)

    url = mock_produce!
    expect(described_class.new(url, klass, cache: cache).count).to eq(300)
    expect(described_class.new(url, klass, cache: cache).count).to eq(300)
  end

  private

  def mock_produce!(enum: mock_enum, **opts)
    url = 'mock:///dir/file.json'
    Feedx::Producer.perform url, enum: enum, **opts
    url
  end

  def mock_enum
    %w[x y z].map {|t| Feedx::TestCase::Model.new(t) } * 100
  end
end
