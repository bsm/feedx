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

  let :relation do
    %w[x y z].map {|_t| model.new('t') } * 100
  end

  let(:tempdir) { Dir.mktmpdir }
  after { FileUtils.rm_rf tempdir }

  it 'should reject invalid inputs' do
    expect do
      described_class.new relation, "file://#{tempdir}/file.txt"
    end.to raise_error(/unable to detect format/)
  end

  it 'should push compressed JSON' do
    pusher = described_class.new relation, "file://#{tempdir}/file.jsonz"
    size   = pusher.perform
    expect(size).to be_within(20).of(140)
    expect(File.size("#{tempdir}/file.jsonz")).to eq(size)
  end

  it 'should push plain JSON' do
    pusher = described_class.new relation, "file://#{tempdir}/file.json"
    size = pusher.perform
    expect(size).to be_within(0).of(15900)
    expect(File.size("#{tempdir}/file.json")).to eq(size)
  end

  it 'should push compressed PB' do
    pusher = described_class.new relation, "file://#{tempdir}/file.pbz"
    size = pusher.perform
    expect(size).to be_within(20).of(41)
    expect(File.size("#{tempdir}/file.pbz")).to eq(size)
  end

  it 'should push plain PB' do
    pusher = described_class.new relation, "file://#{tempdir}/file.pb"
    size = pusher.perform
    expect(size).to be_within(0).of(1200)
    expect(File.size("#{tempdir}/file.pb")).to eq(size)
  end
end
