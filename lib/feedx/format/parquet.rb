require 'parquet'
require 'tmpdir'

class Feedx::Format::Parquet < Feedx::Format::Abstract
  class Record < Arrow::Record
    def each_pair
      container.columns.each do |col|
        yield col.name, col[index]
      end
    end
  end

  class Decoder < Feedx::Format::Abstract::Decoder
    def initialize(io, **)
      super(io)

      @table  = read_table
      @cursor = 0
    end

    def eof?
      @cursor >= @table.n_rows
    end

    def decode(target, **)
      return if eof?

      rec = Record.new(@table, @cursor)
      @cursor += 1

      target = target.allocate if target.is_a?(Class)
      target.from_parquet(rec)
      target
    end

    private

    def read_table
      tmpname = ::Dir::Tmpname.create('feedx-parquet') {|path, *| path }
      IO.copy_stream(@io, tmpname)

      @table = Arrow::Table.load(tmpname, format: 'parquet')
    ensure
      unlink!(tmpname) if tmpname
    end

    def unlink!(tmpname)
      File.unlink(tmpname)
    rescue Errno::ENOENT
      nil
    end
  end

  class Encoder < Feedx::Format::Abstract::Encoder
    attr_reader :schema

    def initialize(io, schema:, buffer_size: 1 << 20, batch_size: 10_000)
      super(io)

      @schema = schema
      @batch_size = batch_size.to_i
      @buffer_size = buffer_size.to_i

      @tmpname = ::Dir::Tmpname.create('feedx-parquet') {|path, *| path }
      @output  = Arrow::FileOutputStream.new(@tmpname, append: false)
      @writer  = Parquet::ArrowFileWriter.new(@schema, @output)
      @batch   = []
    end

    def encode(msg, **opts)
      msg = msg.to_parquet(@schema, **opts) if msg.respond_to?(:to_parquet)

      res = @batch.push(msg)
      flush_table if @batch.size >= @batch_size
      res
    end

    def close
      flush_table unless @batch.empty?

      @writer.close
      @output.close
      IO.copy_stream(@tmpname, @io)
    ensure
      unlink!
    end

    private

    def flush_table
      table = Arrow::RecordBatch.new(@schema, @batch).to_table
      @writer.write_table table, @buffer_size
      @batch.clear
    end

    def unlink!
      File.unlink(@tmpname)
    rescue Errno::ENOENT
      nil
    end
  end
end
