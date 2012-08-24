http              = require "http"
async             = require "async"
uuid              = require "node-uuid"
request           = require "request"
CouchDbEventStore = require "./event_store/couchdb"
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
  @Redis: RedisEventStore

module.exports = EventStore
