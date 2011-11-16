require 'ruote'

module Ruote
  class MongoDbStorage
    include Ruote::StorageBase

    @@collection_prefix = "ruote_"
    @@encoded_dollar_sign = "~#~"

    def initialize(options={})
      super()
      db_config = {"host"=>"localhost", "port"=>27017, "database"=>"Ruote"}
      options = options.dup
      if environment = options.delete(:environment)
        all_db_config=
          File.open(options.delete(:config) || 'config/database.yml','r') do |f|
            YAML.load(f)
          end

        raise "no configuration for environment: #{environment}" unless env_config = all_db_config[environment]
        db_config.merge!(env_config)
      end
      #args take precedent over config
      db_config.merge! options.delete(:connection) if options[:connection]

      @db = Mongo::Connection.new(db_config['host'], db_config['port']).
	db(db_config['database'])
      if db_config['username'] && db_config['password']
        @db.authenticate(db_config['username'], db_config['password'])
      end

      replace_engine_configuration(options)
    end


    def close_connection()
      @db.connection.close()
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


      lock(key, force) do
        
        current_doc = get(type, key)
        current_rev = current_doc ? current_doc['_rev'] : nil

        if current_rev && rev != current_rev
          current_doc
        elsif rev && current_rev.nil?
          true
        else
          nrev = (rev.to_i + 1).to_s
          encoded = to_mongo(doc.merge('_rev' => nrev), with)
          get_collection(type).save(encoded)
          doc['_rev'] = nrev if opts[:update_rev]
          nil
        end
      end
    end
     

    def get(type, key)
      doc = get_collection(type).find_one("_id" => key)
      from_mongo doc if doc
    end


    def delete(doc, opts = {})

      force = (opts.delete(:lock) == false) || opts.delete(:force)
      rev = doc['_rev']
      type = doc['type']
      key = key_for(doc)

      raise ArgumentError.new("can't delete doc without _rev: #{doc.inspect}") unless rev

      lock(key, force) do
        current_doc = get(type, key)
        if current_doc.nil?
          true
        elsif current_doc['_rev'] != rev
          current_doc
        else
          get_collection(type).remove("_id" => key)
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
      @db.collection_names.each do |name| 
        @db.drop_collection(name) if name =~ /^#{@@collection_prefix}/
      end
    end

    def add_type(type)
      get_collection(type).create_index("_id")
    end

    def purge_type!(type)
      @db.drop_collection(@@collection_prefix + type)
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

    def lock(key, force = false)
     collection = get_collection("locks")
      begin
        have_lock = false
        until have_lock do
          collection.remove({"_id" => key, "time" => {"$lte" => Time.now.utc - 60}})
            #expire lock if appropriate
          
          lock = collection.find_and_modify({
            :query  => {"_id" => key },
            :update => {"_id" => key },
            :upsert => true
          })

          unless lock && lock["_id"]
            #locking succesful. We need to timestamp it so it can expire 
            #if we don't finish with it in a reasonable time period
            collection.update({"_id" => key}, { "time" => Time.now.utc })
            have_lock = true
          end
          
        end unless force

        result = yield

        result
      ensure
        collection.remove({"_id" => key})
      end
    end

    def get_collection(type)
      @db[@@collection_prefix + type]
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

      conf = get('configurations', 'engine')

      doc = opts.merge('type' => 'configurations', '_id' => 'engine')
      doc['_rev'] = conf['_rev'] if conf

      put(doc)
    end

  end
end
