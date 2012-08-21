async       = require "async"
uuid        = require "node-uuid"
request     = require "request"
Event       = require "../event"
EventStore  = require "../event_store"

class CouchDbEventStore extends EventStore

  constructor: () ->

  setup: (callback) =>
    async.series [@_setupDatabase, @_setupViews], callback

  createNewUid: (callback) ->
    uid = uuid.v4()
    callback null, uid

  findAllByAggregateUid: (aggregateUid, callback) ->
    request
      uri: @_urlToDocument("_design/events/_view/byAggregate?key=\"#{aggregateUid}\"")
      json: {}
    , (err, response, body) ->
      if err?
        callback err
      else
        events = []
        rows = body.rows.sort (a, b) ->
          a.value.timestamp - b.value.timestamp

        for row in rows
          eventObject = row.value
          event       = new Event eventObject.name, eventObject.data
          event.uid   = eventObject.uid
          events.push event
        callback null, events

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
            callback err || new Error("Couldn't persist event")
          else
            callback null, event

  _urlToDatabase: (database = "albums_development") ->
    "http://localhost:5984/#{database}"

  _urlToDocument: (document) ->
    "#{@_urlToDatabase()}/#{document}"

  _setupDatabase: (callback) =>
    request
      method: "put"
      uri:    @_urlToDatabase()
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
