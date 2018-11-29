require 'spec_helper'

RSpec.describe Feedx::Pusher do
  let :model do
    Class.new Struct.new(:title) do
      def to_pb
        Feedx::TestCase::Message.new title: title
      end

      def to_json
        ::JSON.dump(title: title, updated_at: Time.at(1515151515).utc)
      end
    end
  end

  let :enumerable do
    %w[x y z].map {|t| model.new(t) } * 100
  end

  let(:bucket) { BFS::Bucket::InMem.new }
  before { allow(BFS).to receive(:resolve).and_return(bucket) }

  it 'should reject invalid inputs' do
    expect do
      described_class.perform 'mock:///dir/file.txt', enum: enumerable
    end.to raise_error(/unable to detect format/)
  end

  it 'should push compressed JSON' do
    size = described_class.perform 'mock:///dir/file.jsonz', enum: enumerable
    expect(size).to be_within(20).of(166)
    expect(bucket.info('dir/file.jsonz').size).to eq(size)
  end

  it 'should push plain JSON' do
    size = described_class.perform 'mock:///dir/file.json', enum: enumerable
    expect(size).to eq(15900)
    expect(bucket.info('dir/file.json').size).to eq(size)
  end

  it 'should push compressed PB' do
    size = described_class.perform 'mock:///dir/file.pbz', enum: enumerable
    expect(size).to be_within(20).of(41)
    expect(bucket.info('dir/file.pbz').size).to eq(size)
  end

  it 'should push plain PB' do
    size = described_class.perform 'mock:///dir/file.pb', enum: enumerable
    expect(size).to eq(1200)
    expect(bucket.info('dir/file.pb').size).to eq(size)
  end

  it 'should support factories' do
    size = described_class.perform('mock:///dir/file.json') do
      enumerable
    end
    expect(size).to eq(15900)
    expect(bucket.info('dir/file.json').size).to eq(size)
  end

  it 'should support last-modified' do
    described_class.perform 'mock:///dir/file.json', last_modified: Time.at(1515151515), enum: enumerable
    expect(bucket.info('dir/file.json').metadata).to eq('x-feedx-pusher-last-modified' => '1515151515000')
  end

  it 'should perform conditionally' do
    size = described_class.perform 'mock:///dir/file.json', last_modified: Time.at(1515151515), enum: enumerable
    expect(size).to eq(15900)

    size = described_class.perform 'mock:///dir/file.json', last_modified: Time.at(1515151515), enum: enumerable
    expect(size).to eq(-1)

    size = described_class.perform 'mock:///dir/file.json', last_modified: Time.at(1515151514), enum: enumerable
    expect(size).to eq(-1)

    size = described_class.perform 'mock:///dir/file.json', last_modified: Time.at(1515151516), enum: enumerable
    expect(size).to eq(15900)
  end
end
