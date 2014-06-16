CouchDbEventStore    = require "./event_store/couchdb"
MongoDbEventStore    = require "./event_store/mongodb"
PostgresqlEventStore = require "./event_store/postgresql"

module.exports =
  CouchDb    : CouchDbEventStore
  MongoDb    : MongoDbEventStore
  Postgresql : PostgresqlEventStore
