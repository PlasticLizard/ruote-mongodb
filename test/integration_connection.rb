#
# testing ruote-mongodb
#

require 'yajl'
require 'rufus-json'
Rufus::Json.detect_backend
dir = File.expand_path(File.dirname(__FILE__))

require File.join(dir, '../lib/ruote-mongodb')


class RrLogger
  def method_missing (m, *args)
    super if args.length != 1
    puts ". #{Time.now.to_f} #{Thread.current.object_id} #{args.first}"
  end
end


def new_storage(opts = {})

  Ruote::MongoDbStorage.new(opts)

end
