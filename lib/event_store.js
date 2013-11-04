var CouchDbEventStore = require("./event_store/couchdb");
var MongoDbEventStore = require("./event_store/mongodb");

module.exports = {
  CouchDb : CouchDbEventStore,
  MongoDb : MongoDbEventStore
};