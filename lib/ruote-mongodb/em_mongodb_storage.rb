require 'ruote'
require "eventmachine"
require "em-synchrony"

module Ruote
  class EMMongoDbStorage < MongoDbStorage
    
    protected
    #lock is usually a blocking operation
    #but we don't want that in the event loop
    def wait_for_lock(key, block, collection)
      EM::Synchrony.sync wait_for_lock_async(key)
      lock(key, true, &block)
    end

    def wait_for_lock_async(key, response = nil)
      response ||= EventMachine::DefaultDeferrable.new
      if try_lock(key)
        response.succeed(true)
      else
        EM.next_tick do
          Fiber.new do
            wait_for_lock_async(key, response)
          end.resume
        end
      end
      response
    end

  end
end
