redis       = require "redis"
async       = require "async"
Event       = require "../event"

class EventStore
  constructor: () ->
    @client = redis.createClient()
    @client.on 'error', (error) ->
      throw error

  setup: (callback) ->
    @client.flushdb (err) ->
      callback err

  createNewUid: (callback) ->
    @client.incr 'uid', callback

  findAllByAggregateUid: (aggregateUid, options, callback) ->
    callback = options unless callback?

    @client.lrange "aggregate-timeline:#{aggregateUid}", 0, -1, (err, eventIds) =>

      findEventById = (eventUid, callback) =>
        @client.get "event:#{eventUid}", (err, eventName) =>
          @client.hgetall "event:#{eventUid}:data", (err, eventData) ->
            if err
              callback err
            else
              event = new Event eventName, eventData
              callback null, event

      async.map eventIds, findEventById, callback

  saveEvent: (event, callback) ->
    @createNewUid (err, eventUid) =>
      event.uid = eventUid

      transaction = @client.multi [
        ["set", "event:#{eventUid}", event.name],
        ["hmset", "event:#{eventUid}:data", event.data],
        ["rpush", "event-timeline", eventUid],
        ["rpush", "aggregate-timeline:#{event.aggregateUid}", eventUid]
      ]
      transaction.exec (err, replies) ->
        if err? then callback err else callback null, event

module.exports = EventStore
