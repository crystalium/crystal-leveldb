module LevelDB
  class DB
    getter :db_ptr, :roptions_ptr

    @snapshot : Snapshot?

    def initialize(@path : String, create_if_missing : Bool = true, compression : Bool = true)
      @err_address = 0_u64
      @err_ptr = pointerof(@err_address).as(Pointer(UInt64))

      @options_ptr = LibLevelDB.leveldb_options_create
      LibLevelDB.leveldb_options_set_create_if_missing(@options_ptr, create_if_missing)
      if compression
        LibLevelDB.leveldb_options_set_compression(@options_ptr, LibLevelDB::Compression::SNAPPY_COMPRESSION)
      else
        LibLevelDB.leveldb_options_set_compression(@options_ptr, LibLevelDB::Compression::NO_COMPORESSEION)
      end

      @woptions_ptr = LibLevelDB.leveldb_writeoptions_create
      @roptions_ptr = LibLevelDB.leveldb_readoptions_create

      @db_ptr = LibLevelDB.leveldb_open(@options_ptr, @path, @err_ptr)
      check_error!
      @opened = true
    end

    def put(key : String, value : String) : Void
      ensure_opened!
      LibLevelDB.leveldb_put(@db_ptr, @woptions_ptr, key, key.bytesize, value, value.bytesize, @err_ptr)
      check_error!
    end

    def []=(key, val)
      put(key, val)
    end

    def get(key : String) : String | Nil
      ensure_opened!

      vallen = 0_u64
      valptr = LibLevelDB.leveldb_get(@db_ptr, @roptions_ptr, key, key.bytesize, pointerof(vallen), @err_ptr)
      check_error!
      if valptr.address == 0 || valptr == Pointer(UInt8).null
        return nil
      else
        valstr = String.new(valptr, vallen)
        LibLevelDB.leveldb_free(valptr) # Should free after use. See https://github.com/google/leveldb/blob/1.23/include/leveldb/c.h#L93
        return  valstr
      end
    end

    def [](key)
      get(key)
    end

    def delete(key : String) : Void
      ensure_opened!
      LibLevelDB.leveldb_delete(@db_ptr, @woptions_ptr, key, key.bytesize, @err_ptr)
      check_error!
    end

    def write(batch : Batch)
      ensure_opened!
      LibLevelDB.leveldb_write(@db_ptr, @woptions_ptr, batch.batch_ptr, @err_ptr)
      LibLevelDB.leveldb_writebatch_destroy(batch.batch_ptr)
      check_error!
    end

    def close : Void
      return if closed?
      LibLevelDB.leveldb_close(@db_ptr)
      @opened = false
    end

    def open : Void
      return if opened?
      @db_ptr = LibLevelDB.leveldb_open(@options_ptr, @path, @err_ptr)
      check_error!
      @opened = true
    end

    def destroy : Void
      close
      LibLevelDB.leveldb_destroy_db(@options_ptr, @path, @err_ptr)
      check_error!
    end

    def create_snapshot : Snapshot
      snapshot_ptr = LibLevelDB.leveldb_create_snapshot(@db_ptr)
      Snapshot.new(self, snapshot_ptr)
    end

    def set_snapshot(snapshot : Snapshot) : Void
      raise Error.new("Snapshot does not match database") unless snapshot.db == self
      raise Error.new("Snapshot already released") if snapshot.released?
      LibLevelDB.leveldb_readoptions_set_snapshot(@roptions_ptr, snapshot.__ptr)

      # Keep reference, so if snapshot is in use, it won't be garbage collected
      @snapshot = snapshot
    end

    def unset_snapshot : Void
      LibLevelDB.leveldb_readoptions_set_snapshot(@roptions_ptr, Pointer(Void).null)
      @snapshot = nil
    end

    def opened? : Bool
      @opened
    end

    def closed? : Bool
      !opened?
    end

    def each : Void
      iterator = Iterator.new(self)
      iterator.seek_to_first
      while iterator.valid?
        yield(iterator.key, iterator.value)
        iterator.next
      end
      iterator.destroy
    end

    def clear
      destroy
      open
    end

    def finalize
      close if opened? # closing frees @db_ptr automatically
      LibLevelDB.leveldb_free(@options_ptr)
      LibLevelDB.leveldb_free(@woptions_ptr)
      LibLevelDB.leveldb_free(@roptions_ptr)
    end

    @[AlwaysInline]
    private def ensure_opened!
      raise Error.new("LevelDB #{@path} is closed.") if closed?
    end

    @[AlwaysInline]
    private def check_error!
      if @err_address != 0
        ptr = Pointer(UInt8).new(@err_address)
        message = String.new(ptr)
        LibLevelDB.leveldb_free(ptr)
        @err_address = 0_u64
        @err_ptr = pointerof(@err_address).as(Pointer(UInt64))
        raise(Error.new(message))
      end
    end
  end
end
