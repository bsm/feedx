require 'spec_helper'

RSpec.describe Feedx::Cache::Memory do
  it 'read/writes' do
    expect(subject.fetch('key')).to be_nil
    expect(subject.fetch('key') { 'value' }).to eq('value')
    expect(subject.fetch('key')).to eq('value')
    expect(subject.fetch('key') { 'other' }).to eq('value')
    expect(subject.fetch('key')).to eq('value')

    subject.write('key', 'new-value')
    expect(subject.read('key')).to eq('new-value')
    expect(subject.fetch('key')).to eq('new-value')

    subject.clear
    expect(subject.fetch('key')).to be_nil
  end

  it 'writes strings' do
    subject.write('key', 5)
    expect(subject.read('key')).to eq('5')
  end
end
