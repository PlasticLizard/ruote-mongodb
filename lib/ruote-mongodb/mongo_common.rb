module Ruote
  module MongoCommon
    
    attr_reader :connection, :database_name
    attr_accessor :safe
    
    def self.connection=(connection)
      @connection=connection
    end   

    def database_name=(database_name)
      @database_name = database_name
    end
         
    # @api public
    def database
      database_name ? connection.db(database_name) : nil
    end

    def collection(collection_name)
      db = database
      raise "No database specified" unless db
      collection_name ? db.collection(collection_name) : nil
    end

    def connect(options={})
      options = options.dup
      host = options.delete(:host) || '127.0.0.1'
      port = options.delete(:port) || 27017
      username = options.delete(:username)
      password = options.delete(:password)
      database_name = options.delete(:database) || 'Ruote'
      
      @connection = create_connection(host, port, options)
      self.database_name = database_name

      if username && password && database
        authenticate_database(username, password)
      end   
      @connection                
    end    

    def ensure_indexes
      (::Ruote::MongoDbStorage::TYPES - %w[ msgs schedules ]).each do |t|
        collection(t).ensure_index('_wfid')
        collection(t).ensure_index([ [ '_id', 1 ], [ '_rev', 1 ] ])
      end
      collection("schedules").ensure_index("at")
      collection("expressions").ensure_index("fei.wfid")
    end  

    protected

    def authenticate_database(username, password)
      database.authenticate(username, password)
    end

    def create_connection(host, port, options)
      raise "Create Connection must be implmeneted by a subclass"
    end   
        
  end
end