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

  let(:tempdir) { Dir.mktmpdir }
  after { FileUtils.rm_rf tempdir }

  it 'should reject invalid inputs' do
    expect do
      described_class.perform "file://#{tempdir}/file.txt", enum: enumerable
    end.to raise_error(/unable to detect format/)
  end

  it 'should push compressed JSON' do
    size = described_class.perform "file://#{tempdir}/file.jsonz", enum: enumerable
    expect(size).to be_within(20).of(166)
    expect(File.size("#{tempdir}/file.jsonz")).to eq(size)
  end

  it 'should push plain JSON' do
    size = described_class.perform "file://#{tempdir}/file.json", enum: enumerable
    expect(size).to eq(15900)
    expect(File.size("#{tempdir}/file.json")).to eq(size)
  end

  it 'should push compressed PB' do
    size = described_class.perform "file://#{tempdir}/file.pbz", enum: enumerable
    expect(size).to be_within(20).of(41)
    expect(File.size("#{tempdir}/file.pbz")).to eq(size)
  end

  it 'should push plain PB' do
    size = described_class.perform "file://#{tempdir}/file.pb", enum: enumerable
    expect(size).to eq(1200)
    expect(File.size("#{tempdir}/file.pb")).to eq(size)
  end

  it 'should support factories' do
    size = described_class.perform("file://#{tempdir}/file.json") do
      enumerable
    end
    expect(size).to eq(15900)
    expect(File.size("#{tempdir}/file.json")).to eq(size)
  end
end
