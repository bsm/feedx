require 'spec_helper'

RSpec.describe Feedx::Producer do
  let :enumerable do
    %w[x y z].map {|t| Feedx::TestCase::Model.new(t) } * 100
  end

  let(:bucket) { BFS::Bucket::InMem.new }

  before { allow(BFS).to receive(:resolve).and_return(bucket) }

  it 'rejects invalid inputs' do
    expect do
      described_class.perform 'mock:///dir/file.txt', enum: enumerable
    end.to raise_error(/unable to detect format/)
  end

  it 'pushes compressed JSON' do
    size = described_class.perform 'mock:///dir/file.jsonz', enum: enumerable
    expect(size).to be_within(20).of(166)
    expect(bucket.info('dir/file.jsonz').size).to eq(size)
  end

  it 'pushes plain JSON' do
    size = described_class.perform 'mock:///dir/file.json', enum: enumerable
    expect(size).to eq(15900)
    expect(bucket.info('dir/file.json').size).to eq(size)
  end

  it 'pushes compressed PB' do
    size = described_class.perform 'mock:///dir/file.pbz', enum: enumerable
    expect(size).to be_within(20).of(41)
    expect(bucket.info('dir/file.pbz').size).to eq(size)
  end

  it 'pushes plain PB' do
    size = described_class.perform 'mock:///dir/file.pb', enum: enumerable
    expect(size).to eq(1200)
    expect(bucket.info('dir/file.pb').size).to eq(size)
  end

  it 'supports factories' do
    size = described_class.perform('mock:///dir/file.json') do
      enumerable
    end
    expect(size).to eq(15900)
    expect(bucket.info('dir/file.json').size).to eq(size)
  end

  it 'supports last-modified' do
    described_class.perform 'mock:///dir/file.json', version: 33, enum: enumerable
    expect(bucket.info('dir/file.json').metadata).to eq('X-Feedx-Version' => '33')
  end

  it 'performs conditionally' do
    size = described_class.perform 'mock:///dir/file.json', version: 33, enum: enumerable
    expect(size).to eq(15900)

    size = described_class.perform 'mock:///dir/file.json', version: 33, enum: enumerable
    expect(size).to eq(-1)

    size = described_class.perform 'mock:///dir/file.json', version: 22, enum: enumerable
    expect(size).to eq(-1)

    size = described_class.perform 'mock:///dir/file.json', version: 44, enum: enumerable
    expect(size).to eq(15900)
  end

  it 'accepts downstream options' do
    expect do
      described_class.perform 'mock:///dir/file.jsonz', enum: enumerable, x: 1, y: 'v', z: true
    end.not_to raise_error
  end
end
