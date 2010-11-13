#require "spec_helper"
require 'rubygems'
require 'mongo'
require 'lib/mongodb_storage'
require 'date'

describe Ruote::MongoDbStorage do
  before :each do
    @repo = Ruote::MongoDbStorage.new
    @repo.purge!
  end

  it "can store and retrieve a document by ID" do
    key = BSON::ObjectId.new.to_s
    doc = {"_id" => key, "name" => "ralph", "type" => "test"}
    result = @repo.put doc
    result.should be_nil
    doc = @repo.get 'test', key
    doc["name"].should == "ralph"
  end

  it "can update a document by ID" do
    key = BSON::ObjectId.new.to_s
    doc = {"_id" => key, "name" => "ralph", "type" => "test"}
    @repo.put doc
    doc = @repo.get 'test', key
    doc["name"] = "bill"
    @repo.put doc
    doc = @repo.get 'test', key
    doc["name"].should == "bill"
  end

  it "can store documents with keys starting with dollar sign" do
    key = BSON::ObjectId.new.to_s
    doc = {"_id" => key, "type" => "test", "a" => ["$b" => "c"]}
    @repo.put doc
    doc = @repo.get 'test', key
    doc["a"].should == ["$b" => "c"]
  end

  it "can store documents with dates" do
    key = BSON::ObjectId.new.to_s
    doc = {"_id" => key, "type" => "test", "a" => ["b" => Date.parse("11/9/2010")]}
    @repo.put doc
    doc = @repo.get 'test', key
    doc["a"][0]["b"].to_s.should == "2010-11-09"
  end

  it "can store large floating point numbers accurately" do
    key = BSON::ObjectId.new.to_s
    doc = {"_id" => key, "type" => "test", "raw" => 1289501850.34665} #1289443610.7243}
    @repo.put doc
    doc = @repo.get 'test', key
    doc["raw"].should == 1289501850.34665 #1289443610.7243
  end

  it "can retrieve a document by a string ID" do
    key = "hello"
    doc = {"_id" => key, "name" => "ralph", "type" => "test"}
    @repo.put doc
    doc = @repo.get 'test', key
    doc["name"].should == "ralph"
  end

  it "can delete a document" do
    key = BSON::ObjectId.new.to_s
    doc = {"_id" => key, "name" => "ralph", "type" => "test", "_rev" => 0}
    @repo.put doc
    @repo.delete doc
    @repo.get('test', key).should be_nil
  end

  it "will provide a list of IDs" do
    key1 = "hello" + BSON::ObjectId.new.to_s
    key2 = "hello" + BSON::ObjectId.new.to_s
    key3 = "hello" + BSON::ObjectId.new.to_s
    @repo.put({"_id" => key1, "name" => "ralph", "type" => "test"})
    @repo.put({"_id" => key2, "name" => "ralph", "type" => "test"})
    @repo.put({"_id" => key3, "name" => "ralph", "type" => "test2"})
    @repo.ids("test").should == [key1, key2]
    @repo.ids("test2").should == [key3]
  end

  it "only purges collections starting with the ruote_ prefix" do
    db = @repo.instance_eval "@db"
    db.drop_collection("something_else")
    @repo.put({"_id" => BSON::ObjectId.new.to_s, "name" => "ralph", "type" => "test"})
    @repo.put({"_id" => BSON::ObjectId.new.to_s, "name" => "bill", "type" => "test2"})
    db.collection_names.should == ["system.indexes", "ruote_test", "ruote_test2"]
    db["something_else"].insert({"name" => "doug"})
    @repo.purge!
    db.collection_names.should == ["system.indexes", "something_else"]
  end

  it "can purge a particular type" do
    key1 = BSON::ObjectId.new.to_s
    key2 = BSON::ObjectId.new.to_s
    @repo.put({"_id" => key1, "name" => "ralph", "type" => "test"})
    @repo.put({"_id" => key2, "name" => "bill", "type" => "test2"})
    @repo.purge_type! "test2"
    @repo.get('test', key1)["name"].should == "ralph"
    @repo.get('test2', key2).should be_nil
  end

  describe "can get multiple documents" do
    before :each do
      key1 = "TANGO!ALPHA!BRAVO"
      key2 = "TANGO!ALPHA-BRAVO"
      key3 = "FOXTROT!ALPHA-BRAVO"
      @repo.put({"_id"=>key1, "fname"=>"ralph", "lname"=>"A", "type"=>"test"})
      @repo.put({"_id"=>key2, "fname"=>"bill", "lname"=>"B", "type"=>"test"})
      @repo.put({"_id"=>key3, "fname"=>"nancy", "lname"=>"A", "type"=>"test"})
    end

    it "with criteria" do
      search_key = ["BRAVO", /FOXTROT/]
      docs = @repo.get_many("test", search_key)
      docs.count.should == 2
      (docs.select {|doc| doc["fname"] == "ralph"}).count.should == 1
      (docs.select {|doc| doc["fname"] == "nancy"}).count.should == 1
      (docs.select {|doc| doc["fname"] == "bill"}).count.should == 0
    end

    it "without criteria" do
      docs = @repo.get_many("test", nil)
      docs.count.should == 3
    end

    it "up to a certain limit" do
      docs = @repo.get_many("test", nil, {:limit => 2})
      docs.count.should == 2
      (docs.select {|doc| doc["fname"] == "nancy"}).count.should == 1
      (docs.select {|doc| doc["fname"] == "ralph"}).count.should == 1
    end

    it "skipping a certain number" do
      docs = @repo.get_many("test", nil, {:skip => 2})
      docs.count.should == 1
      (docs.select {|doc| doc["fname"] == "bill"}).count.should == 1
    end

    it "in descending order" do
      docs = @repo.get_many("test", nil, {:descending => true})
      docs[0]["fname"].should == "bill"
      docs[1]["fname"].should == "ralph"
      docs[2]["fname"].should == "nancy"
    end

    it "in ascending order" do
      docs = @repo.get_many("test", nil, {:descending => false})
      docs[0]["fname"].should == "nancy"
      docs[1]["fname"].should == "ralph"
      docs[2]["fname"].should == "bill"
    end

    it "in asceneding order, by default" do
      docs = @repo.get_many("test", nil)
      docs[0]["fname"].should == "nancy"
      docs[1]["fname"].should == "ralph"
      docs[2]["fname"].should == "bill"
    end

    it "count" do
      @repo.get_many("test", nil, {:count => true}).should == 3
    end
  end
end
