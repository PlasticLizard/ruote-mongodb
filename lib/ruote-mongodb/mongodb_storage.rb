require 'ruote'
require 'date'

module Ruote
  class MongoDbStorage
    include StorageBase
    include MonitorMixin

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

      @db = Mongo::Connection.new(db_config['host'], db_config['port'], 
        :safe=>true).db(db_config['database'])
      if db_config['username'] && db_config['password']
        @db.authenticate(db_config['username'], db_config['password'])
      end

      unless get('configurations','engine')
        put(options.merge('type'=>'configurations', '_id'=>'engine')) 
      end
    end

    def close_connection()
      @db.connection.close()
    end

    def put(doc, opts={})
      synchronize do
        raise "doc must have a type" unless doc['type']
        raise "doc must have an ID" unless doc['_id']
        pre = get(doc['type'], doc['_id'])

        if pre && pre['_rev'] != doc['_rev']
          return pre
        end

        if pre.nil? && doc['_rev']
          return true
        end

        doc = if opts[:update_rev]
                doc['_rev'] = pre ? pre['_rev'] : -1
                doc
              else
                doc.merge('_rev' => doc['_rev'] || -1)
              end

        doc['put_at'] = Ruote.now_to_utc_s
        doc['_rev'] = doc['_rev'] + 1

        encoded_doc = Rufus::Json.dup(doc)
        to_mongo encoded_doc
        get_collection(doc['type']).save(encoded_doc)

        nil
      end
    end

    def get(type, key)
      synchronize do
        doc = get_collection(type).find_one("_id" => key)
        from_mongo doc if doc
        doc
      end
    end

    def delete(doc)
      drev = doc['_rev']
      raise ArgumentError.new("can't delete doc without _rev") unless drev
      synchronize do
        raise "doc must have a type" unless doc['type']
        prev = get(doc['type'], doc['_id'])
        return true if prev.nil?
        doc['_rev'] ||= 0
        if prev['_rev'] == drev
          get_collection(doc['type']).remove("_id" => doc["_id"])
          nil
        else
          prev
        end
      end
    end

    def get_many(type, key=nil, opts={})
      synchronize do
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
    end

    def ids(type)
      synchronize do
        result = get_collection(type).find({}, {:fields=>["_id"]}).map do |row|
          row["_id"].to_s
        end
        result.sort
      end
    end

    def purge!
      synchronize do
        @db.collection_names.each do |name| 
          @db.drop_collection(name) if name =~ /^#{@@collection_prefix}/
        end
      end
    end

    def add_type(type)
      synchronize do
        get_collection(type).create_index("_id")
      end
    end

    def purge_type!(type)
      synchronize do
        @db.drop_collection(@@collection_prefix + type)
      end
    end

    private

    def get_collection(type)
      @db[@@collection_prefix + type]
    end

    # encodes unsupported data ($, Date) for storage in MongoDB
    def from_mongo(doc)
      mongo_encode(doc, /^#{@@encoded_dollar_sign}/, "$", :backward)
    end

    # unencodes unsupported values ($, Date) from storage in MongoDB
    def to_mongo(doc)
      mongo_encode(doc, /^\$/, @@encoded_dollar_sign, :forward)
    end

    # called by from_mongo and to_mongo
    def mongo_encode(doc, pattern, replacement, date_conv)
      if doc.is_a? Hash
        doc.each_pair do |key, value|
          new_key = key
          if key.is_a?(String) && key =~ pattern
            new_key = key.sub(pattern, replacement)
            doc[new_key] = value
            doc.delete key
          end
          mongo_encode(value, pattern, replacement, date_conv)
          ensure_date_encoding(value, doc, new_key, date_conv)
          doc[new_key] = value.to_s if value.is_a? Symbol
        end
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

    #TODO: REMOVE once this provider is considered stable
    def diff(search_key, result1, result2, f, level = 0)
      f.write("#{level}: search key: #{search_key}\n") if level == 0
      f.write("SAME\n") and return false if (!result1 && !result2) || result1 == result2
      f.write("DIFF(#{level}): result1 is nil but result2 = #{result2.inspect}\n") and return if result2 && !result1
      f.write("DIFF(#{level}): result2 is nil but result1 = #{result1.inspect}\n") and return if result1 && !result2
      f.write("DIFF(#{level}): class of result1 is #{result1.class} but result2 is a #{result2.class}\n") and return if result1.class != result2.class && !(result1.class == BSON::OrderedHash && result2.class == Hash)
      return if level > 3 #consider removing if performs adequitely

      if result1.is_a? Array
        result1.each_with_index do |entry, i|
          diff search_key, entry, result2[i], f, level + 1
        end
      elsif result1.is_a? Hash
      if result1.keys.count != result2.keys.count
        result1.each_pair do |key, value|
          f.write "DIFF(#{level}): result2 doesn't have: #{key.inspect}=#{value.inspect}\n" unless result2[key]
        end
        result2.each_pair do |key, value|
          f.write "DIFF(#{level}): result1 doesn't have: #{key.inspect}=#{value.inspect}\n" unless result1[key]
        end
      end

      result1.each_pair do |key, value|
        other_value = result2[key]
        
        if other_value && value != other_value && !['put_at', 'created_time', '_rev'].include?(key)
          if value.is_a?(Hash) || value.is_a?(Array)
            diff(search_key, value, other_value, f, level + 1)
          else
            f.write "DIFF(#{level}): result1 has #{key.inspect} = #{value.inspect}\n...but result2 has #{other_value.inspect}\n"
          end
        end
      end
      else
        f.write "DIFF(#{level}): values differ: #{result1} / #{result2}\n"
      end

      return true
    end
  end
end
