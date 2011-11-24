require "ruote-mongodb/mongo_common"

begin
  require "em-mongo"
rescue LoadError => error
  raise "Missing dependency: gem install em-mongo"
end

begin
  require "em-synchrony"
  require "em-synchrony/em-mongo"
rescue LoadError => error
  raise "Missing dependency: gem install em-synchrony"
end

module Ruote
  class EMMongo
    include MongoCommon
    
    def self.connect(options = {})
      mongo = new
      mongo.connect(options)
      mongo
    end
    #em-mongo does not internally pool connections,
    #so self.connection might be a em-syncrhony
    #collection pool.
    #For that reason, we are not caching the database
    #in the adapter as, unlike with the mongo-ruby driver,
    #it will not nessarily be associated with the 
    #current connection        
    def database
      database_name ? connection.db(database_name) : nil
    end   

    protected

    def create_connection(host, port, options = {})
      timeout = options.delete(:timeout)
      EM::Mongo::Connection.new(host, port, timeout, options)
    end     

  end
end

