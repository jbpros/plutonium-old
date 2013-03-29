url         = require "url"
async       = require "async"
uuid        = require "node-uuid"

Event       = require "../event"
EventStore  = require "../event_store"
Profiler    = require "../profiler"
MongoClient = require("mongodb").MongoClient

class MongoDbEventStore extends EventStore

  MONGODB_DUPLICATE_KEY_ERROR_CODE = 11000
  MIN_RETRY_DELAY                  = 1
  MAX_RETRY_DELAY                  = 15
  MAX_RETRIES                      = 10

  constructor: ({@uri, @logger}) ->
    throw new Error "Missing URI" unless @uri
    throw new Error "Missing logger" unless @logger

    @collectionName = "EventCollection"

  initialize: (callback) ->
    MongoClient.connect @uri, (err, db) =>
      return @_closeConnectionAndReturn db, err, callback if err?
      db.createCollection @collectionName, (err, collection) =>
        return @_closeConnectionAndReturn db, err, callback if err?
        @db         = db
        @collection = collection
        callback null

  destroy: (callback) ->
    @_closeConnectionAndReturn @db, null, (err) =>
      @db         = null
      @collection = null
      callback null

  _closeConnectionAndReturn: (db, err, callback) ->
    db.close() if db?
    callback err

  setup: (callback) ->
    @collection.remove (err, result) =>
      return callback err if err?
      @collection.ensureIndex {"aggregateUid": 1}, (err, result) =>
        return callback err if err?
        @collection.ensureIndex {"aggregateUid": 1, "version": 1}, { unique: true }, callback

  createNewUid: (callback) ->
    uid = uuid.v4()
    callback null, uid

  findAll: (callback) ->
    @_find {}, callback

  findAllByAggregateUid: (aggregateUid, callback) ->
    @_find aggregateUid: aggregateUid, callback

  saveEvent: (event, callback) =>
    @createNewUid (err, eventUid) =>
      return callback err if err?
      event.uid = eventUid
      payload =
        uid: eventUid
        name: event.name
        aggregateUid: event.aggregateUid
        timestamp: event.timestamp
        data: {}
        _attachments: {}

      for key, value of event.data
        if value instanceof Buffer
          payload["_attachments"][key] = value
        else
          payload["data"][key] = value

      params  = aggregateUid: event.aggregateUid
      options =
        fields:
          version: 1
        sort:
          version: -1
        limit: 1

      getCurrentVersion = (callback) =>
        @collection.find(params, options).toArray (err, items) =>
          return callback err if (err)

          item = items[0]
          version = if item then item.version + 1 else 1
          callback null, version

      tries = 0

      tryToPersist = =>
        tries++
        getCurrentVersion (err, version) =>
          payload.version = version

          @collection.insert payload, {w:1}, (err, result) =>
            if (err && err.code != MONGODB_DUPLICATE_KEY_ERROR_CODE)
              return callback err
            else if (err)
              if (tries == MAX_RETRIES)
                @logger.critical "MongoDbEventStore#saveEvent", "concurrency situation - Reached maximum retries while persisting event #{eventUid} for aggregate #{event.aggregateUid}"
                return callback new Error "Reached maximum retries while persisting event #{eventUid} for aggregate #{event.aggregateUid}"
              else
                retryDelay = Math.floor(Math.random() * (MAX_RETRY_DELAY - MIN_RETRY_DELAY + 1)) + MIN_RETRY_DELAY
                @logger.warning "MongoDbEventStore#saveEvent", "concurrency situation - retrying after #{retryDelay}ms"
                setTimeout tryToPersist, retryDelay
            else
              event.version = version
              callback err, event

      tryToPersist()

  _find: (params, callback) ->
    p = new Profiler "MongoDbEventStore#_find(db request)", @logger
    p.start()
    @collection.find(params).sort("version":1).toArray (err, items) =>
      p.end()

      if err?
        callback err
      else if not items?
        callback null, []
      else
        @_instantiateEventsFromRows items, callback

  _instantiateEventsFromRows: (rows, callback) ->
    events = []
    return callback null, events if rows.length is 0

    rowsQueue = async.queue (row, rowCallback) =>
      uid          = row.uid
      name         = row.name
      aggregateUid = row.aggregateUid
      data         = row.data
      timestamp    = row.timestamp
      version      = row.version

      @_loadAttachmentsFromRow row, (err, attachments) ->
        return rowCallback err if err?

        for attachmentName, attachmentBody of attachments
          data[attachmentName] = attachmentBody

        event = new Event
          name: name
          data: data
          uid: uid
          aggregateUid: aggregateUid
          timestamp: timestamp
          version: version

        events.push event
        process.nextTick rowCallback
    , 1

    rowsQueue.drain = ->
      callback null, events

    rowsQueue.push rows

  _loadAttachmentsFromRow: (row, callback) ->
    attachments = {}
    for attachmentName, attachmentBody of row._attachments
      attachments[attachmentName] = attachmentBody.buffer

    callback null, attachments

module.exports = MongoDbEventStore