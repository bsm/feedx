require 'zlib'

class Feedx::Compression::Gzip < Feedx::Compression::Abstract
  def self.wrap(io, &block)
    Zlib::GzipWriter.wrap(io, &block)
  end
end
