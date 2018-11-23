require 'spec_helper'

RSpec.describe Feedx::TaskState do
  subject { described_class }
  after   { subject.clear }

  it 'should run with state' do
    subject.with('blank') do |state|
      expect(state).to eq({})
    end

    subject.with('sums', default: { sum: 0 }) do |state|
      expect(state).to eq(sum: 0)
    end

    rnd = Random.new(101)
    10.times do
      subject.with('sums') {|s| s[:sum] += rnd.rand(10) }
    end
    expect(subject['sums']).to eq(sum: 53)
  end
end
