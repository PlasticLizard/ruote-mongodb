require 'ruote'

module Ruote
  class MongoDbStorage
    include Ruote::StorageBase

    TYPES = %w[
      msgs schedules expressions workitems errors
      configurations variables trackers history locks
    ]

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
      schedules.map { |s| from_mongo s}
    end

    def put_schedule(flavor, owner_fei, s, msg)

      doc = prepare_schedule_doc(flavor, owner_fei, s, msg)

      return nil unless doc

      r = put(doc)

      raise "put_schedule failed" if r != nil

      doc['_id']

    end


    def put(doc, opts={})
      if opts[:force]
        get_collection(doc['type']).save(doc, :safe => true)
        return nil
      end

      original = doc
      doc = doc.dup

      doc['_rev'] = (doc['_rev'].to_i || -1) + 1
      doc['put_at'] = Ruote.now_to_utc_s

      collection = get_collection(doc['type'])

      r = begin
        collection.update(
          {
            '_id' => doc['_id'],
            '_rev' => original['_rev']
          },
          to_mongo(doc),
          :safe => true,
          :upsert => original['_rev'].nil?
        )
      rescue Exception => ex
        false
      end

      #em mongo does not return a hash - it just returns true if succesful
      #and false otherwise, thus the r ==  true clause.
      if r && ( r== true || (r['updatedExisting'] || original['_rev'].nil?))
        original['_rev'] = doc['_rev'] if opts[:update_rev]
        nil
      else
        collection.find_one('_id' => doc['_id']) || true
      end
    end


    def get(type, key, collection = nil)
      collection ||= get_collection(type)
      doc = collection.find_one("_id" => key)
      from_mongo doc if doc
    end


    def delete(doc, opts = {})

      if opts[:force]
        get_collection(doc['type']).remove({'_id' => doc['_id']}, :safe => true)
        return nil
      end

      rev = doc['_rev']

      raise ArgumentError.new("can't delete doc without _rev") unless rev

      collection = get_collection(doc['type'])

      r = collection.remove(
        { '_id' => doc['_id'], '_rev' => doc['_rev'] },
        :safe => true)

      if r == true || r['n'] == 1
        nil
      else
        collection.find_one('_id' => doc['_id']) || true
      end

    end

    def find_root_expression(wfid)
      root = get_collection("expressions").find_one("fei.wfid" => wfid, "parent_id" => nil)
      from_mongo(root) if root
    end

    def get_many(type, key=nil, opts={})
      opts = Ruote.keys_to_s(opts)
      keys = key ? Array(key) : nil
      collection = get_collection(type)

      cursor = if keys.nil?
        collection.find
      elsif keys.first.is_a?(Regexp)
        collection.find('_id' => { '$in' => keys })
      else # a String
        collection.find('fei.wfid' => { '$in' => keys })
      end

      return cursor.count if opts['count']

      cursor.sort(
        '_id', opts['descending'] ? ::Mongo::DESCENDING : ::Mongo::ASCENDING)

      cursor.skip(opts['skip'])
      cursor.limit(opts['limit'])

      cursor.to_a.map{|d|from_mongo d}
    end

    def ids(type)
      get_collection(type).find({}, {:fields=>["_id"]}).map do |row|
        row["_id"]
      end
    end

    def purge!
      TYPES.each { |t| get_collection(t).remove }
    end

    def add_type(type)
    end

    def purge_type!(type)
      begin
        @mongo.database.drop_collection(Ruote::MongoCommon::COLLECTION_PREFIX + type)
      rescue
      end
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

    def if_lock(key)
      collection = get_collection("locks")
      unless collection.find_and_modify(:query => {"_id" => key}, :update => {"_id" => key})
        begin
          yield
          return true
        ensure
          collection.remove("_id" => key)
        end
      end
      false
    end

    protected

    def get_collection(type)
      mongo.collection(type)
    end

    def from_mongo(doc)
      rekey(doc) { |k| k.gsub(/^~#~/, '$').gsub(/~_~/, '.') }
    end

    def to_mongo(doc, with = {})
      doc.merge(with).merge!("put_at" => Ruote.now_to_utc_s)
      rekey(doc) { |k| k.to_s.gsub(/^\$/, '~#~').gsub(/\./, '~_~') }
    end

    def rekey(o, &block)
      case o
        when Hash; o.remap { |(k, v), h| h[block.call(k)] = rekey(v, &block) }
        when Array; o.collect { |e| rekey(e, &block) }
        when Symbol; o.to_s
        else o
      end
    end
  end
end
