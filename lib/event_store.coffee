http              = require "http"
async             = require "async"
uuid              = require "node-uuid"
CouchDbEventStore = require "./event_store/couchdb"
MongoDbEventStore = require "./event_store/mongodb"
RedisEventStore   = require "./event_store/redis"

class EventStore

  setup: (callback) =>
    throw new Error "Implement me"

  createNewUid: (callback) =>
    throw new Error "Implement me"

  findAll: (options, callback) ->
    throw new Error "implement me"

  findAllByAggregateUid: (aggregateUid, options, callback) ->
    throw new Error "Implement me"

  saveEvent: (event, callback) =>
    throw new Error "Implement me"

  @CouchDb: CouchDbEventStore
  @MongoDb: MongoDbEventStore
  @Redis: RedisEventStore

module.exports = EventStore