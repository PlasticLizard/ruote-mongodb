require 'ruote'
require 'date'

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
      schedules.each { |s| from_mongo s}
      filter_schedules(schedules, now)
    end
  
    def put(doc, opts={})
      
      force = (opts.delete(:lock) == false) || opts.delete(:force)
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
          doc = if opts[:update_rev]
                  doc['_rev'] = current_rev || -1
                  doc
                else
                  doc.merge('_rev' => doc['_rev'] || -1)
                end

          doc['put_at'] = Ruote.now_to_utc_s
          doc['_rev'] = doc['_rev'] + 1

          encoded_doc = Rufus::Json.dup(doc)
          to_mongo encoded_doc
          get_collection(type).save(encoded_doc)
          nil
        end
      end
    end
     

    def get(type, key)
      doc = get_collection(type).find_one("_id" => key)
      from_mongo doc if doc
      doc
    end


    def delete(doc, opts = {})

      force = (opts.delete(:lock) == false) || opts.delete(:force)
      rev = doc['_rev']
      type = doc['type']
      key = key_for(doc)

      raise ArgumentError.new("can't delete doc without _rev") unless rev

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
      docs.each do |doc|
        from_mongo doc
      end
      docs
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

    # encodes unsupported data ($, Date) for storage in MongoDB
    def from_mongo(doc)
      doc
      #mongo_encode(doc, /^#{@@encoded_dollar_sign}/, "$", :backward)
    end

    # unencodes unsupported values ($, Date) from storage in MongoDB
    def to_mongo(doc)
      doc
      #mongo_encode(doc, /^\$/, @@encoded_dollar_sign, :forward).merge!('put_at' => Ruote.now_to_utc_s)
    end

    # called by from_mongo and to_mongo
    def mongo_encode(doc, pattern, replacement, date_conv)
      doc
      if doc.is_a? Hash
        doc.keys.each do |key|
          new_key = key
          value = doc[key]
          if key.is_a?(String) && key =~ pattern
            new_key = key.sub(pattern, replacement)
            doc[new_key] = value
            doc.delete key
          end
          mongo_encode(value, pattern, replacement, date_conv)
          ensure_date_encoding(value, doc, new_key, date_conv)
          doc[new_key] = value.to_s if value.is_a? Symbol
        end
        doc
      elsif doc.is_a? Array
        doc.each_with_index do |entry, i|
          mongo_encode(entry, pattern, replacement, date_conv)
          ensure_date_encoding(entry, doc, i, date_conv)
          doc[i] = entry.to_s if entry.is_a? Symbol
        end
      end
    end

    def ensure_date_encoding(value, doc, key, date_conv)
      if value.is_a?(Date) && date_conv == :forward
        doc[key] = "DT_" + value.to_s
      end
      if value.is_a?(String) && value[0,3] == "DT_" && date_conv == :backward
        doc[key] = Date.parse(value[3..-1])
      end
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
