Q                = require 'q'
async            = require 'async'
Event            = require './event'

class Entity

  @eventHandlers = {}

  constructor: ->
    @constructor._checkDomainRepositoryInstance()
    @uid           = null
    @appliedEvents = []

  toString: ->
    "[object Entity:#{@constructor.name}]"

  triggerEvent: (eventName, attributes, callback) ->
    @constructor._checkDomainRepositoryInstance()
    event = new Event eventName, attributes
    @applyEvent event, (err) =>
      if err?
        callback err
      else
        event.aggregateUid = @uid
        @appliedEvents.push event
        Entity.domainRepository.add @
        callback null

  applyEvent: (event, callback) ->
    @constructor._checkDomainRepositoryInstance()
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

  @initialize: (domainRepository) =>
    throw new Error "Missing domain repository" unless domainRepository?
    Entity.domainRepository = domainRepository

  @findByUid: (uid, callback) ->
    @_checkDomainRepositoryInstance()
    Entity.domainRepository.findAggregateByUid @, uid, callback

  @buildFromEvents: (events, callback) ->
    @_checkDomainRepositoryInstance()
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
    @_checkDomainRepositoryInstance()
    @domainRepository.createNewUid callback

  @_onEvent: (eventName, callback) =>
    @eventHandlers[eventName] ?= []
    @eventHandlers[eventName].push callback

  @_checkDomainRepositoryInstance: =>
    throw new Error "Domain repository instance not set on constructor" unless @domainRepository?

module.exports = Entity
