require 'ruote'

module Ruote
  class MongoDbStorage
    include Ruote::StorageBase

    COLLECTION_PREFIX = "ruote_"

    attr_reader :mongo

    def initialize(mongo, options = {})
      @mongo = mongo
      replace_engine_configuration(options)
    end

    def close_connection()
      @mongo.connection.close()
    end

    def get_schedules(delta, now)
      nowstr = Ruote.time_to_utc_s(now)
      schedules = get_collection("schedules").find("at" => {"$lte" => nowstr}).to_a
      schedules = schedules.map { |s| from_mongo s}
      filter_schedules(schedules, now)
    end

    def put_schedule(flavor, owner_fei, s, msg)

      doc = prepare_schedule_doc(flavor, owner_fei, s, msg)

      return nil unless doc

      r = put(doc, :with => {:at => doc["at"]})

      raise "put_schedule failed" if r != nil

      doc['_id']

    end

      
    def put(doc, opts={})
      
      force = (opts.delete(:lock) == false) || opts.delete(:force)
      with = opts.delete(:with) || {}

      key = key_for(doc)
      rev = doc['_rev']
      type = doc['type']
      collection = opts.delete(:collection) || get_collection(type)


      lock(key, force) do

        current_doc = get(type, key, collection)
        current_rev = current_doc ? current_doc['_rev'] : nil

        if current_rev && rev != current_rev
          current_doc
        elsif rev && current_rev.nil?
          true
        else
          nrev = (rev.to_i + 1).to_s
          encoded = to_mongo(doc.merge('_rev' => nrev), with)
          collection.save(encoded)
          doc['_rev'] = nrev if opts[:update_rev]
          nil
        end
      end
    end
     

    def get(type, key, collection = nil)
      collection ||= get_collection(type)
      doc = collection.find_one("_id" => key)
      from_mongo doc if doc
    end


    def delete(doc, opts = {})

      force = (opts.delete(:lock) == false) || opts.delete(:force)
      rev = doc['_rev']
      type = doc['type']
      key = key_for(doc)
      collection = get_collection(type)

      raise ArgumentError.new("can't delete doc without _rev: #{doc.inspect}") unless rev

      lock(key, force) do
        current_doc = get(type, key, collection)
        if current_doc.nil?
          true
        elsif current_doc['_rev'] != rev
          current_doc
        else
          collection.remove("_id" => key)
          nil
        end
      end

    end

    def get_many(type, key=nil, opts={})
      return get_collection(type).count if opts[:count]
      criteria = {}
      find_opts = {}
      find_opts[:limit] = opts[:limit] if opts[:limit]
      find_opts[:skip] = opts[:skip] if opts[:skip]
      find_opts[:sort] = ["_id", opts[:descending] ? :descending : :ascending]
      if key
        id_criteria = Array(key).map do |k|
          case k.class.to_s
          when "String" then "!#{k}$"
          when "Regexp" then k.source
          else k.to_s
          end
        end
        criteria = {"_id" => /#{id_criteria.join "|"}/}
      end
      docs = get_collection(type).find(criteria, find_opts).to_a
      docs.map do |doc|
        from_mongo doc
      end
    end

    def ids(type)
      result = get_collection(type).find({}, {:fields=>["_id"]}).map do |row|
        row["_id"].to_s
      end
      result
    end

    def purge!
      @mongo.database.collection_names.each do |name| 
        @mongo.database.drop_collection(name) if name =~ /^#{MongoDbStorage::COLLECTION_PREFIX}/
      end
    end

    def add_type(type)
      get_collection(type).create_index("_id")
    end

    def purge_type!(type)
      @mongo.database.drop_collection(MongoDbStorage::COLLECTION_PREFIX + type)
    end

    def close
      close_connection
    end

    def shutdown
    end

    # Returns a String containing a representation of the current content of
    # in this storage.
    #
    def dump(type)
      get_many(type).map{|d|d.to_s}.sort.join("\n")
    end


    protected
 

    def key_for(doc)
      doc['_id']
    end

    def lock(key, force = false, &block)
      collection = get_collection("locks")
      result = nil

      unless force || try_lock(key, collection)
        wait_for_lock(key, block, collection)
      end

      begin
        result = yield
      ensure
        #better release the lock, come rain or shine
        collection.remove({"key" => key})
      end
      result      
    end

    def try_lock(key, collection = nil)
      collection ||= get_collection("locks")
      collection.remove({"key" => key, "time" => {"$lte" => Time.now.utc - 60}})
        #expire lock if appropriate
      
      lock = collection.find_and_modify({
        :query  => {"key" => key },
        :update => {"$set" => {"key" => key }, "$inc" => {"requests" => 1 } },
        :upsert => true
      })
      if lock.nil? || lock.empty?
        #locking succesful. We need to timestamp it so it can expire 
        #if we don't finish with it in a reasonable time period
        collection.update({"key" => key}, { "$set" => {"time" => Time.now.utc }})
        true
      else
        false
      end
    end

    #We're putting the blocking wait into a separate method
    #so that the event machine version can do its own thing
    def wait_for_lock(key, block, collection = nil)
      loop do 
        sleep 0.01
      end until try_lock(key, collection)
      lock(key, true, &block)
    end

    def get_collection(type)
      mongo.collection(MongoDbStorage::COLLECTION_PREFIX + type)
    end

    def from_mongo(doc)
      s = doc['document']
      doc = s ? Rufus::Json.decode(s) : nil
      doc
    end

    def to_mongo(doc, with = {})
      {"_id" => doc["_id"], "document" => Rufus::Json.encode(doc.merge('put_at' => Ruote.now_to_utc_s))}.merge!(with)
    end
  
    
    #this method is in the ruote master but not the released gem
    def replace_engine_configuration(opts)

      return if opts['preserve_configuration']

      type = 'configurations'
      collection = get_collection(type)      

      conf = get(type, 'engine', collection)

      doc = opts.merge('type' => type, '_id' => 'engine')
      doc['_rev'] = conf['_rev'] if conf

      put(doc, :collection => collection)
    end

  end
end
