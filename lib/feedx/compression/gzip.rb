require 'zlib'

class Feedx::Compression::Gzip < Feedx::Compression::Abstract
  def reader(io, **, &block)
    force_binmode(io)
    Zlib::GzipReader.wrap(io, &block)
  end

  def writer(io, **, &block)
    force_binmode(io)
    Zlib::GzipWriter.wrap(io, &block)
  end

  private

  def force_binmode(io)
    if io.respond_to?(:binmode)
      io.binmode
    elsif io.respond_to?(:set_encoding)
      io.set_encoding(Encoding::BINARY)
    end
  end
end
