require "ruote-mongodb/mongo_common"
require "ruote-mongodb/mongodb_storage"

begin
  require "mongo"
rescue LoadError => error
  raise "Missing dependency: gem install em-mongo"
end

module Ruote
  class Mongo
    include MongoCommon

    def self.connect(options = {})
      mongo = new
      mongo.connect(options)
      mongo
    end
   
    protected

    def create_connection(host, port, options = {})
      ::Mongo::Connection.new(host, port, options)
    end     

    def create_repl_set_connection(hosts, options = {})
      ::Mongo::ReplSetConnection.new(*hosts.push(options))
    end

  end
end

