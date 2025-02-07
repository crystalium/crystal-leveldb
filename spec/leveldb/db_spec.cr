require "../spec_helper"
require "wait_group"

describe LevelDB do
  describe "DB" do
    it "works" do
      FileUtils.rm_r(TEST_DB) if Dir.exists?(TEST_DB)

      db = LevelDB::DB.new(TEST_DB)

      # get / put
      db.put("key", "value")
      db.get("key").should eq "value"

      # when key does not exist
      db.get("something-else").should eq nil

      # delete
      db.delete("key")
      db.get("key").should eq nil

      # Try to open already opened DB
      expect_raises(LevelDB::Error, "IO error: lock") do
        LevelDB::DB.new(TEST_DB)
      end

      # close
      db.close

      # should not raise, cause DB was closed
      db = LevelDB::DB.new(TEST_DB)
      db.close

      # destroy
      db.destroy
    end

    describe ".new" do
      context "when DB does not exist yet" do
        context "when create_if_missing = true" do
          it "opens DB" do
            db = LevelDB::DB.new(TEST_DB, create_if_missing: true)
            db.close
            db.destroy
          end
        end

        context "when create_if_missing = false" do
          it "raises exception" do
            expect_raises(LevelDB::Error, "does not exist (create_if_missing is false)") do
              db = LevelDB::DB.new(TEST_DB, create_if_missing: false)
            end
          end
        end
      end
    end

    describe "snapshots" do
      it "can create, set and unset snapshots" do
        db = LevelDB::DB.new(TEST_DB)
        db.put("aa", "11")
        db.put("bb", "22")

        snapshot = db.create_snapshot

        db.delete("aa")
        db.get("aa").should eq nil
        db.get("bb").should eq "22"

        # Set snapshot
        db.set_snapshot(snapshot)
        db.get("aa").should eq "11"
        db.get("bb").should eq "22"

        db.unset_snapshot
        db.get("aa").should eq nil
        db.get("bb").should eq "22"

        # Release snapshot
        snapshot.release
        expect_raises(LevelDB::Error, "Snapshot already released") do
          db.set_snapshot(snapshot)
        end
        db.get("aa").should eq nil
        db.get("bb").should eq "22"

        db.unset_snapshot

        db.close
      end
    end

    it "has #open/#close methods" do
      FileUtils.rm_r(TEST_DB) if Dir.exists?(TEST_DB)
      db = LevelDB::DB.new(TEST_DB)

      db.opened?.should eq true
      db.closed?.should eq false

      # Opening an opened DB should not cause exceptions
      db.open

      db.put("x", "33")

      db.close
      db.opened?.should eq false
      db.closed?.should eq true

      # Try operations on closed DB
      expect_raises(LevelDB::Error, "is closed") { db.get("x") }
      expect_raises(LevelDB::Error, "is closed") { db.put("x", "32") }
      expect_raises(LevelDB::Error, "is closed") { db.delete("x") }

      # Make sure double close does not cause exceptions
      db.close
      db.close
    end

    it "supports #[], #[]= methods" do
      FileUtils.rm_r(TEST_DB) if Dir.exists?(TEST_DB)
      db = LevelDB::DB.new(TEST_DB)
      db["name"] = "Sergey"
      db["name"].should eq "Sergey"
      db.close
    end

    describe "#each" do
      it "iterates through all the keys" do
        FileUtils.rm_r(TEST_DB) if Dir.exists?(TEST_DB)
        db = LevelDB::DB.new(TEST_DB)

        out = ""

        db.put("k1", "v1")
        db.put("k2", "v2")
        db.put("k3", "v3")

        db.each do |key, val|
          out += "#{key}=#{val};"
        end

        out.should eq "k1=v1;k2=v2;k3=v3;"

        db.close
      end
    end

    describe "#clear" do
      it "removes all the keys" do
        FileUtils.rm_r(TEST_DB) if Dir.exists?(TEST_DB)
        db = LevelDB::DB.new(TEST_DB)

        db.put("k1", "v1")
        db.put("k2", "v2")

        db.clear
        db.get("k1").should eq nil
        db.get("k2").should eq nil

        db.close
      end
    end

    describe "on stess load" do
      it "works" do
        FileUtils.rm_r(TEST_DB) if Dir.exists?(TEST_DB)
        db = LevelDB::DB.new(TEST_DB)
        stress_amount = ENV.fetch("BASIC_STRESS_AMOUNT") { 1000 }.to_u64
        data_length = 64*1024
        key_length = 1024

        data = "a" * data_length
        key = "a" * key_length

        stress_amount.times do
          db.put(key, data)
          db.get(key).should eq data

          data = data.succ
          key = key.succ
        end
        
        key = "a" * key_length
        stress_amount.times do
          data = db.get(key)
          data.should_not eq nil
          if data
            data.size.should eq data_length
          end
          key = key.succ
        end
                
        stress_amount.times do |n|
          key = "empty:#{n}"
          db.get(key).should eq nil
        end

        db.destroy        
      end

      it "with lots of errors" do
        db = LevelDB::DB.new(TEST_DB)

        db.opened?.should eq true
        db.closed?.should eq false

        db.open

        db.put("x", "33")
        db.get("x").should eq "33"

        db.close
        db.opened?.should eq false
        db.closed?.should eq true

        x=1.to_s
        stress_amount = ENV.fetch("ERRORS_STRESS_AMOUNT") { 10000 }.to_u64
        stress_amount.times do |n|
          expect_raises(LevelDB::Error, "is closed") { db.get("x") }
          expect_raises(LevelDB::Error, "is closed") { db.put("x", "32") }
          expect_raises(LevelDB::Error, "is closed") { db.delete("x") }
        end
      end

      it "works with lots of errors in multiple fibers" do
        db = LevelDB::DB.new(TEST_DB)

        db.opened?.should eq true
        db.closed?.should eq false
          
        db.open
  
        db.put("x", "33")
        db.get("x").should eq "33"
  
        db.close
        db.opened?.should eq false
        db.closed?.should eq true
  
        fibers_count = ENV.fetch("MT_ERRORS_FIBERS_COUNT") { 16 }.to_i
        stress_amount = ENV.fetch("MT_ERRORS_STRESS_AMOUNT") { 1000 }.to_i
        wg = WaitGroup.new(fibers_count)
        fibers_count.times do
          spawn do
            stress_amount.times do
              expect_raises(LevelDB::Error, "is closed") { db.get("x") }
              expect_raises(LevelDB::Error, "is closed") { db.put("x", "32") }
              expect_raises(LevelDB::Error, "is closed") { db.delete("x") }
            end
          ensure
            wg.done
          end
        end    

        wg.wait
      end

      it "with snapshots" do
        FileUtils.rm_r(TEST_DB) if Dir.exists?(TEST_DB)
        db = LevelDB::DB.new(TEST_DB)

        db.put("aa", "11")
        db.put("bb", "22")

        base_snapshot = db.create_snapshot

        db.put("cc", "--")

        base_snapshot_with_c = db.create_snapshot

        db.delete("aa")
        db.get("aa").should eq nil
        db.get("bb").should eq "22"
        
        stress_amount = ENV.fetch("SNAPSHOTS_STRESS_AMOUNT") { 1000 }.to_i
        snapshots = Array(LevelDB::Snapshot).new
        data = "aaaaa"
        stress_amount.times do |n|
          db.unset_snapshot
          db.get("cc").should_not eq n.to_s
          db.put("cc", n.to_s)
          snp = db.create_snapshot
          snapshots.push snp
          db.get("cc").should eq n.to_s
          db.set_snapshot(snp)
          db.get("cc").should eq n.to_s
        end

        n = 0
        snapshots.each do |snp|
          db.get("cc").should_not eq n.to_s
          db.unset_snapshot
          db.set_snapshot(snp)
          db.get("cc").should eq n.to_s
          n += 1
        end

        db.set_snapshot(base_snapshot)

        db.get("aa").should eq "11"
        db.get("bb").should eq "22"
        db.get("cc").should eq nil

        db.unset_snapshot

        db.set_snapshot(base_snapshot_with_c)

        db.get("cc").should eq "--"

        db.destroy
      end
    end
  end
end
