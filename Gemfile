require 'rubygems'
source 'http://rubygems.org'

gemspec

gem "bson", ">= 1.2.0"
gem "bson_ext", ">= 1.2.0"

group :mongo do
	gem "mongo", ">= 1.2.0"
end

group :mongo_async do
	gem "eventmachine", ">= 1.0.0.beta.3"
	gem "em-mongo"
	gem "em-synchrony", :git => "https://github.com/PlasticLizard/em-synchrony.git"
end