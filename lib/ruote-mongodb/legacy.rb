module Ruote
  class MongoDbStorage

  	def from_mongo(doc)
  		encoded_dollar_sign = "~#~"
      mongo_encode(doc, /^#{encoded_dollar_sign}/, "$", :backward)
      doc
    end

     # called by from_mongo and to_mongo
    def mongo_encode(doc, pattern, replacement, date_conv)
      if doc.is_a? Hash
        doc.keys.each do |key|
          value = doc[key]
          new_key = key
          if key.is_a?(String)
            repalce_key = false
            if key =~ /\./
              new_key = key.gsub(".","_")
              repalce_key = true
            end
            if new_key =~ pattern
              new_key = new_key.sub(pattern, replacement)
              repalce_key = true
            end
            if repalce_key
              # puts "======= MongoDbStorage#mongo_encode: replace key from #{key} to #{new_key}"
              doc[new_key] = value
              doc.delete key
            end
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

    # To be called on doc to be saved back to mongodb to ensure that keys don't contain $ and . characters
    def mongo_encode_key(doc)
      if doc.is_a?(Hash)
        doc.keys.each do |key|
          new_key = key
          value = doc[key]
          if key.is_a?(String)
            if new_key =~ /(\$|\.)/
              new_key = new_key.gsub(".","_")
              new_key = new_key.sub("$","~#~")
              # puts "======= MongoDbStorage#mongo_encode_key: replace #{key} with #{new_key}"
              doc[new_key] = value
              doc.delete key
            end
          end
          mongo_encode_key(value)
          doc[new_key] = value.to_s if value.is_a? Symbol
        end
      elsif doc.is_a? Array
        doc.each_with_index do |entry, i|
          mongo_encode_key(entry)
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


  end
end