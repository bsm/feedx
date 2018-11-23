require 'spec_helper'

RSpec.describe Feedx::Pusher::Recurring do
  let :enumerable do
    enum = %w[x y z].map {|t| { title: t } } * 100
    enum.instance_eval do
      def maximum(*)
        1515151515.0
      end
    end
    enum
  end

  let(:tempdir) { Dir.mktmpdir }
  after { FileUtils.rm_rf tempdir }
  after { described_class.registry.clear }

  it 'should perform conditionally' do
    size = described_class.perform 'test.task', "file://#{tempdir}/file.json", enum: enumerable
    expect(size).to eq(4200)
    expect(File.size("#{tempdir}/file.json")).to eq(size)
    expect(described_class.registry).to include('test.task')
    expect(described_class.registry['test.task']).to include(revision: 1515151515.0)

    size = described_class.perform 'test.task', "file://#{tempdir}/file.json", enum: enumerable
    expect(size).to eq(-1)
  end
end
