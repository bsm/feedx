require 'spec_helper'

RSpec.describe Feedx::Stream do
  subject { described_class.new('mock:///dir/file.json') }

  let(:bucket) { BFS::Bucket::InMem.new }
  let(:compressed) { described_class.new('mock:///dir/file.json.gz') }

  before  { allow(BFS).to receive(:resolve).and_return(bucket) }

  after { subject.close; compressed.close }

  it 'rejects invalid inputs' do
    expect do
      described_class.new('mock:///dir/file.txt')
    end.to raise_error(/unable to detect format/)
  end

  it 'accepts custom formats' do
    format = Class.new do
      def encoder(io, &block)
        Feedx::Format::JSON::Encoder.open(io, &block)
      end

      def decoder(io, &block)
        Feedx::Format::JSON::Decoder.open(io, &block)
      end
    end

    result = described_class.open('mock:///dir/file.txt', format: format.new) do |stream|
      stream.create {|s| s.encode Feedx::TestCase::Model.new('X') }
      21
    end
    expect(result).to eq(21)

    expect(bucket.read('dir/file.txt')).to eq(<<~JSON)
      {"title":"X","updated_at":"2018-01-05 11:25:15 UTC"}
    JSON
  end

  it 'encodes' do
    subject.create do |s|
      s.encode(Feedx::TestCase::Model.new('X'))
      s.encode(Feedx::TestCase::Model.new('Y'))
    end

    expect(bucket.read('dir/file.json')).to eq(<<~JSON)
      {"title":"X","updated_at":"2018-01-05 11:25:15 UTC"}
      {"title":"Y","updated_at":"2018-01-05 11:25:15 UTC"}
    JSON
  end

  it 'encodes compressed' do
    compressed.create do |s|
      100.times do
        s.encode(Feedx::TestCase::Model.new('X'))
      end
    end

    expect(bucket.info('dir/file.json.gz').size).to be_within(10).of(108)
  end

  it 'encodes with create options' do
    subject.create metadata: { 'x' => '5' } do |s|
      s.encode(Feedx::TestCase::Model.new('X'))
    end
    expect(bucket.info('dir/file.json').metadata).to eq('X' => '5')
  end

  it 'aborts encode on errors (if compressed)' do
    stop = RuntimeError.new('STOP')
    expect do
      compressed.create do |s|
        s.encode(Feedx::TestCase::Model.new('X'))
        raise stop
      end
    end.to raise_error(stop)

    expect(bucket.ls('**').to_a).to be_empty
  end

  it 'decodes' do
    subject.create do |s|
      s.encode(Feedx::TestCase::Model.new('X'))
      s.encode(Feedx::TestCase::Model.new('Y'))
    end

    subject.open do |s|
      expect(s.decode(Feedx::TestCase::Model)).to eq(Feedx::TestCase::Model.new('X'))
      expect(s.decode(Feedx::TestCase::Model)).to eq(Feedx::TestCase::Model.new('Y'))
      expect(s.decode(Feedx::TestCase::Model)).to be_nil
      expect(s).to be_eof
    end
  end

  it 'decodes compressed' do
    compressed.create do |s|
      s.encode(Feedx::TestCase::Model.new('X'))
      s.encode(Feedx::TestCase::Model.new('Y'))
    end

    compressed.open do |s|
      expect(s.decode(Feedx::TestCase::Model)).to eq(Feedx::TestCase::Model.new('X'))
      expect(s.decode(Feedx::TestCase::Model)).to eq(Feedx::TestCase::Model.new('Y'))
      expect(s.decode(Feedx::TestCase::Model)).to be_nil
      expect(s).to be_eof
    end
  end
end
