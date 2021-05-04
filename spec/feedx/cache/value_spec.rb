require 'spec_helper'

RSpec.describe Feedx::Cache::Value do
  subject do
    described_class.new(Feedx::Cache::Memory.new, 'key')
  end

  it 'read/writes' do
    expect(subject.fetch).to be_nil
    expect(subject.fetch { 'value' }).to eq('value')
    expect(subject.fetch).to eq('value')
    expect(subject.fetch { 'other' }).to eq('value')
    expect(subject.fetch).to eq('value')

    subject.write('new-value')
    expect(subject.read).to eq('new-value')
    expect(subject.fetch).to eq('new-value')
  end
end
