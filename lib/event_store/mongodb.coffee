url         = require "url"
async       = require "async"
uuid        = require "node-uuid"
Event       = require "../event"
Snapshot    = require "../snapshot"
Base        = require "./base"
Profiler    = require "../profiler"
MongoClient = require("mongodb").MongoClient
defer       = require "../defer"

class MongoDbEventStore extends Base

  MONGODB_DUPLICATE_KEY_ERROR_CODE = 11000
  MIN_RETRY_DELAY                  = 1
  MAX_RETRY_DELAY                  = 15
  MAX_RETRIES                      = 10

  constructor: ({@uri, @logger}) ->
    throw new Error "Missing URI" unless @uri
    throw new Error "Missing logger" unless @logger

    @eventCollectionName    = "events"
    @snapshotCollectionName = "snapshots"

  initialize: (callback) ->
    async.waterfall [
      (next) =>
        MongoClient.connect @uri, next
      (db, next) =>
        @db = db
        @db.createCollection @eventCollectionName, next
      (eventCollection, next) =>
        @eventCollection = eventCollection
        @db.createCollection @snapshotCollectionName, next
      (snapshotCollection, next) =>
        @snapshotCollection = snapshotCollection
        next()
    ], (err) =>
      if err?
        @_closeConnectionAndReturn @db, err, callback
      else
        callback null

  destroy: (callback) ->
    @_closeConnectionAndReturn @db, null, (err) =>
      @db                 = null
      @eventCollection    = null
      @snapshotCollection = null
      callback null

  _closeConnectionAndReturn: (db, err, callback) ->
    db.close() if db?
    callback err

  setup: (callback) ->
    async.series [
      (next) =>
        @eventCollection.remove next
      (next) =>
        @eventCollection.ensureIndex {"aggregateUid": 1}, next
      (next) =>
        @eventCollection.ensureIndex {"aggregateUid": 1, "version": 1}, { unique: true }, next
      (next) =>
        @snapshotCollection.remove next
      (next) =>
        @snapshotCollection.ensureIndex {"aggregateUid": 1}, next
    ], callback

  createNewUid: (callback) ->
    uid = uuid.v4()
    callback null, uid

  findAllEvents: (callback) ->
    p = new Profiler "MongoDbEventStore#_find(db request)", @logger
    p.start()
    @eventCollection.find({}).sort("timestamp":1).toArray (err, items) =>
      p.end()

      if err?
        callback err
      else if not items?
        callback null, []
      else
        @_instantiateEventsFromRows items, callback

  findAllEventsByAggregateUid: (aggregateUid, callback) ->
    @_find aggregateUid: aggregateUid, callback

  countAllEventsByAggregateUid: (aggregateUid, callback) ->
    @_count aggregateUid: aggregateUid, callback

  findSomeEventsByAggregateUidBeforeVersion: (aggregateUid, version, eventCount, callback) ->
    @_findLimited { aggregateUid: aggregateUid, version: { "$lte": version } }, eventCount, callback

  findAllEventsByAggregateUidAfterVersion: (aggregateUid, version, callback) ->
    @_find aggregateUid: aggregateUid, version: { $gt: version }, callback

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
        @eventCollection.find(params, options).toArray (err, items) =>
          return callback err if (err)

          item = items[0]
          version = if item then item.version + 1 else 1
          callback null, version

      tries = 0

      tryToPersist = =>
        tries++
        getCurrentVersion (err, version) =>
          payload.version = version

          @eventCollection.insert payload, {w:1}, (err, result) =>
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

  loadSnapshotForAggregateUid: (uid, callback) ->
    @snapshotCollection.findOne aggregateUid: uid, (err, item) ->
      if err?
        callback err
      else if item
        snapshot = new Snapshot item
        callback null, snapshot
      else
        callback null, null

  saveSnapshot: (snapshot, callback) ->
    @snapshotCollection.update aggregateUid: snapshot.aggregateUid, snapshot, w: 1, upsert: 1, (err) =>
      if err?
        @logger.alert "MongoDbEventStore#saveSnapshot", "failed to save snapshot of aggregate \"#{snapshot.aggregateUid}\": #{err}"
      else
        @logger.log "MongoDbEventStore#saveSnapshot", "saved snapshot for aggregate \"#{snapshot.aggregateUid}\""
      callback? err

  _findLimited: (params, eventCount, callback) ->
    p = new Profiler "MongoDbEventStore#_findLimited(db request)", @logger
    p.start()
    @eventCollection.find(params).sort("version": -1).limit(eventCount).toArray (err, items) =>
      p.end()

      if err?
        callback err
      else if not items?
        callback null, []
      else
        @_instantiateEventsFromRows items, callback

  _find: (params, callback) ->
    p = new Profiler "MongoDbEventStore#_find(db request)", @logger
    p.start()
    @eventCollection.find(params).sort("version":1).toArray (err, items) =>
      p.end()

      if err?
        callback err
      else if not items?
        callback null, []
      else
        @_instantiateEventsFromRows items, callback

  _count: (params, callback) ->
    @eventCollection.find(params).count (err, number) ->
      return callback err if err?
      callback null, number

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
        defer rowCallback
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