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
      collection("locks").create_index("key")
      collection("locks").create_index([["key",1],["time",1]])
      collection("schedules").create_index([["at",1]])
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