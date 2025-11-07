require 'zlib'

class Feedx::Compression::Gzip < Feedx::Compression::Abstract
  def reader(io, **, &)
    force_binmode(io)
    Zlib::GzipReader.wrap(io, &)
  end

  def writer(io, **, &)
    force_binmode(io)
    Zlib::GzipWriter.wrap(io, &)
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
