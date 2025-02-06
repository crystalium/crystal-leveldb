module LevelDB
  class Snapshot
    getter :db, :__ptr

    def initialize(@db : DB, @__ptr : Pointer(Void))
      @released = false
    end

    def released?
      @released
    end

    def release
      raise Error.new("Snapshot already released") if @released

      LibLevelDB.leveldb_release_snapshot(@db.db_ptr, @__ptr)
      @released = true
    end

    def finalize
      release unless released?
    end
  end
end
