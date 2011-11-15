#ruote functional tests rely on inspecting Ruby hashes, so 
#our BSON hashes need to inspect as the same so the tests pass.
module BSON
  class OrderedHash
    def inspect
      "{#{entries.map{|e|"#{e[0].inspect}=>#{e[1].inspect}"}.join(",")}}"
    end

    if RUBY_VERSION < '1.9'
      #BSON::OrderedHash bug
      def delete_if
        keys.each do |k|
          if yield k, self[k]
            delete(k)
          end
        end
        self
      end
    end

  end
end