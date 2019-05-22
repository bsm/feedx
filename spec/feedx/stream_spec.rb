require 'spec_helper'

RSpec.describe Feedx::Stream do
  let(:bucket) { BFS::Bucket::InMem.new }
  before  { allow(BFS).to receive(:resolve).and_return(bucket) }

  subject { described_class.new('mock:///dir/file.json') }
  let(:compressed) { described_class.new('mock:///dir/file.json.gz') }

  it 'should reject invalid inputs' do
    expect do
      described_class.new('mock:///dir/file.txt')
    end.to raise_error(/unable to detect format/)
  end

  it 'should encode' do
    subject.create do |s|
      s.encode(Feedx::TestCase::Model.new('X'))
      s.encode(Feedx::TestCase::Model.new('Y'))
    end

    expect(bucket.open('dir/file.json').read).to eq(
      %({"title":"X","updated_at":"2018-01-05 11:25:15 UTC"}\n) +
      %({"title":"Y","updated_at":"2018-01-05 11:25:15 UTC"}\n),
    )
  end

  it 'should encode compressed' do
    compressed.create do |s|
      100.times do
        s.encode(Feedx::TestCase::Model.new('X'))
      end
    end

    expect(bucket.info('dir/file.json.gz').size).to be_within(10).of(108)
  end

  it 'should encode with create options' do
    subject.create metadata: { 'x' => '5' } do |s|
      s.encode(Feedx::TestCase::Model.new('X'))
    end
    expect(bucket.info('dir/file.json').metadata).to eq('x' => '5')
  end

  it 'should decode' do
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

  it 'should decode compressed' do
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
