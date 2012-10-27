Q                = require 'q'
async            = require 'async'
DomainRepository = require './domain_repository'
Event            = require './event'

class Entity

  @eventHandlers = {}

  constructor: ->
    @uid           = null
    @appliedEvents = []

  toString: ->
    "[object Entity:#{@constructor.name}]"

  triggerEvent: (eventName, attributes, callback) ->
    event = new Event eventName, attributes
    @applyEvent event, (err) =>
      if err?
        callback err
      else
        event.aggregateUid = @uid
        @appliedEvents.push event
        DomainRepository.add @
        callback null

  applyEvent: (event, callback) ->
    eventHandlers = Entity.eventHandlers[event.name] || []
    deferredEventHandlers = []
    eventHandlers.forEach (eventHandler) =>
      deferredEventHandler = Q.defer()
      deferredEventHandlers.push deferredEventHandler.promise
      eventHandler.call @, event, deferredEventHandler.makeNodeResolver()

    deferred = Q.all(deferredEventHandlers)
    deferred.then ->
      callback null
    , callback

  @findByUid: (uid, callback) ->
    DomainRepository.findAggregateByUid @, uid, callback

  @buildFromEvents: (events, callback) ->
    entity = new @
    queue = async.queue (event, eventTaskCallback) ->
      entity.applyEvent event, (err) ->
        throw err if err? # todo: don't throw, callback with error (with Q.all()) (or not...)
        eventTaskCallback()
    , 1
    queue.drain = ->
      callback null, entity # todo ensure errors from handlers are passed here
    queue.push events

  @createNewUid: (callback) =>
    DomainRepository.createNewUid callback

  @_onEvent: (eventName, callback) =>
    @eventHandlers[eventName] ?= []
    @eventHandlers[eventName].push callback

module.exports = Entity
