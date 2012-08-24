async       = require "async"
uuid        = require "node-uuid"
request     = require "request"
Event       = require "../event"
EventStore  = require "../event_store"

class CouchDbEventStore extends EventStore
  constructor: (@uri) ->

  setup: (callback) =>
    async.series [@_setupDatabase, @_setupViews], callback

  createNewUid: (callback) ->
    uid = uuid.v4()
    callback null, uid

  findAll: (options, callback) ->
    unless callback?
      callback = options
      options  = {}

    request
      uri: @_urlToDocument("_design/events/_view/byTimestamp?attachments=true")
      json: {}
    , (err, response, body) =>
      if err? # todo: improve errors
        throw err
      else if body.error?
        throw new Error("Error: #{body.error} - #{body.reason}")
      else
        @_instantiateEventsFromRows body.rows, options, callback

  findAllByAggregateUid: (aggregateUid, options, callback) ->
    unless callback?
      callback = options
      options  = {}

    request
      uri: @_urlToDocument("_design/events/_view/byAggregate?key=\"#{aggregateUid}\"")
      json: {}
    , (err, response, body) =>
      if err?
        callback err
      else
        @_instantiateEventsFromRows body.rows, options, callback

  saveEvent: (event, callback) =>
    @createNewUid (err, eventUid) =>
      event.uid = eventUid

      payload =
        uid: event.uid
        name: event.name
        aggregateUid: event.aggregateUid
        timestamp: Date.now()
        data: {}
        _attachments: {}

      for key, value of event.data
        if value instanceof Buffer
          encodedValue = value.toString "base64"
          payload["_attachments"][key] =
            content_type: "application/octet-stream"
            data: encodedValue
        else
          payload['data'][key] = value

      request
        method: "put"
        uri:    @_urlToDocument(eventUid)
        json:   payload
      , (err, response, body) ->
          if err? or not body.ok
            callback err or new Error("Couldn't persist event (#{body.error} - #{body.reason})")
          else
            callback null, event

  _urlToDocumentAttachment: (document, attachment) ->
    "#{@_urlToDocument document}/#{attachment}"

  _urlToDocument: (document) ->
    "#{@uri}/#{document}"

  _instantiateEventsFromRows: (rows, options, callback) ->
    unless callback?
      callback = options
      options  = {}

    options.loadBlobs ?= false

    rows = rows.sort (a, b) ->
      a.value.timestamp - b.value.timestamp

    events = []
    rowsQueue = async.queue (row, rowCallback) =>
      value        = row.value
      name         = value.name
      uid          = value.uid
      aggregateUid = value.aggregateUid
      data         = value.data
      attachments  = value._attachments

      pushEvent = (callback) ->
        event              = new Event name, data
        event.uid          = uid
        event.aggregateUid = aggregateUid
        events.push event
        callback()

      if options.loadBlobs and attachments?
        attachmentsQueue = async.queue (attachment, attachmentCallback) =>
          request
            uri: @_urlToDocumentAttachment uid, attachment
            encoding: null # i.e. buffer
          , (err, response, body) =>
            if err? # todo: improve errors
              throw err
            else if body.error?
              throw new Error("Error: #{body.error} - #{body.reason}")
            else
              data[attachment] = body
              attachmentCallback()
        , 1

        attachmentsQueue.drain = ->
          pushEvent rowCallback

        for k, _ of attachments
          attachmentsQueue.push k
      else
        pushEvent rowCallback
    , 1

    rowsQueue.drain = ->
      callback null, events

    rowsQueue.push rows

  _setupDatabase: (callback) =>
    request
      method: "delete"
      uri: @uri
      json: {}
    , =>
      request
        method: "put"
        uri:    @uri
        json:   {}
      , (err, response, body) ->
          if err?
            callback err
          else if body.ok or (not body.ok and body.error is "file_exists")
            callback null
          else
            callback new Error "Couldn't create database; unknown reason (#{body})"

  _setupViews: (callback) =>
    async.parallel [
      (callback) =>
        @_setupView "_design/events",
          language: "coffeescript"
          views:
            byAggregate:
              map: "(doc) -> if doc.aggregateUid? then emit doc.aggregateUid, doc"
            byAggregateEventCount:
              map: "(doc) -> if doc.aggregateUid? then emit doc.aggregateUid, 1"
              reduce: "_sum"
            byTimestamp:
              map: "(doc) -> if doc.aggregateUid? then emit doc.timestamp, doc"
        , callback
    ], callback

  _setupView: (document, view, callback) =>
    request
      method: "put"
      uri:    @_urlToDocument(document)
      json:  view
    , (err, response, body) ->
        if err?
          callback err
        else if body.ok
          callback null
        else
          callback new Error "Couldn't create view; unknown reason (#{body.error})"

module.exports = CouchDbEventStore
