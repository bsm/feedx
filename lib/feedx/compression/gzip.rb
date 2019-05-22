require 'zlib'

class Feedx::Compression::Gzip < Feedx::Compression::Abstract
  def self.reader(io, &block)
    Zlib::GzipReader.wrap(io, &block)
  end

  def self.writer(io, &block)
    Zlib::GzipWriter.wrap(io, &block)
  end
end
