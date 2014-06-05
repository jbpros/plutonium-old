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
      return callback err if err?
      @db                 = null
      @eventCollection    = null
      @snapshotCollection = null
      callback null

  _closeConnectionAndReturn: (db, err, callback) ->
    if db?
      db.close (closeErr) ->
        return callback closeErr if closeErr?
        callback err
    else
      callback err

  setup: (callback) ->
    async.series [
      (next) =>
        @eventCollection.remove next
      (next) =>
        @eventCollection.ensureIndex {entityUid: 1}, next
      (next) =>
        @eventCollection.ensureIndex {entityUid: 1, version: 1}, { unique: true }, next
      (next) =>
        @eventCollection.ensureIndex {timestamp: 1, uid: 1}, next
      (next) =>
        @snapshotCollection.remove next
      (next) =>
        @snapshotCollection.ensureIndex {entityUid: 1}, next
    ], callback

  createNewUid: (callback) ->
    uid = uuid.v4()
    callback null, uid

  findAllEventsOneByOne: (options, eventHandler, callback) ->
    [options, eventHandler, callback] = [{}, options, eventHandler] unless callback?

    query = {}
    sortQuery =
      timestamp: 1
      uid: 1

    doFindAll = =>
      p = new Profiler "MongoDbEventStore#_find(db request)", @logger
      p.start()

      cursor = @eventCollection.find(query).sort(sortQuery)
      retrieve = =>
        cursor.nextObject (err, item) =>
          return callback err if err?
          if item?
            @_instantiateEventFromRow item, (err, event) ->
              eventHandler err, event, (err) ->
                return callback err if err?
                retrieve()
          else
            p.end()
            callback null
      retrieve()

    startUid = options.startUid
    if startUid?
      @eventCollection.findOne uid: startUid, (err, event) ->
        return callback err if err?
        startTimestamp = event.timestamp
        query =
          timestamp:
            $gte: startTimestamp
          uid:
            $ne: startUid
        doFindAll()
    else
      doFindAll()

  findAllEventsByEntityUid: (entityUid, order, callback) ->
    [order, callback] = [null, order] unless callback?

    @_find entityUid: entityUid, order, callback

  countAllEventsByEntityUid: (entityUid, callback) ->
    @_count entityUid: entityUid, callback

  findSomeEventsByEntityUidBeforeVersion: (entityUid, version, eventCount, callback) ->
    @_findLimited { entityUid: entityUid, version: { "$lte": version } }, eventCount, callback

  findAllEventsByEntityUidAfterVersion: (entityUid, version, callback) ->
    @_find entityUid: entityUid, version: { $gt: version }, callback

  saveEvent: (event, callback) =>
    @createNewUid (err, eventUid) =>
      return callback err if err?
      event.uid = eventUid
      payload =
        uid: eventUid
        name: event.name
        entityUid: event.entityUid
        timestamp: event.timestamp
        data: {}
        _attachments: {}

      for key, value of event.data
        if value instanceof Buffer
          payload["_attachments"][key] = value
        else
          payload["data"][key] = value

      params  = entityUid: event.entityUid
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
                @logger.critical "MongoDbEventStore#saveEvent", "concurrency situation - Reached maximum retries while persisting event #{eventUid} for entity #{event.entityUid}"
                return callback new Error "Reached maximum retries while persisting event #{eventUid} for entity #{event.entityUid}"
              else
                retryDelay = Math.floor(Math.random() * (MAX_RETRY_DELAY - MIN_RETRY_DELAY + 1)) + MIN_RETRY_DELAY
                @logger.warning "MongoDbEventStore#saveEvent", "concurrency situation - retrying after #{retryDelay}ms"
                setTimeout tryToPersist, retryDelay
            else
              event.version = version
              callback err, event

      tryToPersist()

  loadSnapshotForEntityUid: (uid, callback) ->
    @snapshotCollection.findOne entityUid: uid, (err, item) ->
      if err?
        callback err
      else if item
        snapshot = new Snapshot item
        callback null, snapshot
      else
        callback null, null

  saveSnapshot: (snapshot, callback) ->
    @snapshotCollection.update entityUid: snapshot.entityUid, snapshot, w: 1, upsert: 1, (err) =>
      if err?
        @logger.alert "MongoDbEventStore#saveSnapshot", "failed to save snapshot of entity \"#{snapshot.entityUid}\": #{err}"
      else
        @logger.log "MongoDbEventStore#saveSnapshot", "saved snapshot for entity \"#{snapshot.entityUid}\""
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

  _find: (params, order, callback) ->
    [order, callback] = [null, order] unless callback?

    p = new Profiler "MongoDbEventStore#_find(db request)", @logger
    p.start()
    @eventCollection.find(params).sort("version":(order or 1)).toArray (err, items) =>
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
      @_instantiateEventFromRow row, (err, event) ->
        return callback err if err?
        events.push event
        defer rowCallback
    , 1

    rowsQueue.drain = ->
      callback null, events

    rowsQueue.push rows

  _instantiateEventFromRow: (row, callback) ->
    uid          = row.uid
    name         = row.name
    entityUid = row.entityUid
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
        entityUid: entityUid
        timestamp: timestamp
        version: version

      callback null, event

  _loadAttachmentsFromRow: (row, callback) ->
    attachments = {}
    for attachmentName, attachmentBody of row._attachments
      attachments[attachmentName] = attachmentBody.buffer

    callback null, attachments

module.exports = MongoDbEventStore
