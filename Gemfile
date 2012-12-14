#!/usr/bin/ruby

require 'rubygems'
source 'http://rubygems.org'

gemspec

# The current ruote version (as of 2012/09/14) available in RubyGems is 2.3.0.1
gem "ruote", "~>2.3.0"

gem "bson", "1.5.2"
gem "bson_ext", "1.5.2"

group :mongo do
	gem "mongo", "1.5.2"
end

group :mongo_async do
	gem "eventmachine", ">= 1.0.0.beta.3"
	gem "em-mongo"
	gem "em-synchrony", :git => "https://github.com/PlasticLizard/em-synchrony.git"
end