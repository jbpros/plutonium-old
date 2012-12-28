Q                = require 'q'
async            = require 'async'
Event            = require './event'

class Entity

  @eventHandlers = {}

  constructor: ->
    @constructor._checkDependencies()
    @uid              = null
    @appliedEvents    = []
    @domainRepository = Entity.domainRepository
    @commandBus       = Entity.commandBus
    @logger           = Entity.logger

  toString: ->
    "[object Entity:#{@constructor.name}]"

  triggerEvent: (eventName, attributes, callback) ->
    @constructor._checkDependencies()
    event = new Event eventName, attributes
    @applyEvent event, (err) =>
      return callback err if err?
      event.aggregateUid = @uid
      @appliedEvents.push event
      @domainRepository.add @
      callback null

  applyEvent: (event, callback) ->
    @constructor._checkDependencies()
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

  @initialize: (options) =>
    throw new Error "Missing domain repository" unless options.domainRepository?
    throw new Error "Missing command bus" unless options.commandBus?
    throw new Error "Missing logger" unless options.logger?
    Entity.domainRepository = options.domainRepository
    Entity.commandBus = options.commandBus
    Entity.logger = options.logger

  @findByUid: (uid, callback) ->
    @_checkDependencies()
    Entity.domainRepository.findAggregateByUid @, uid, callback

  @buildFromEvents: (events, callback) ->
    @_checkDependencies()
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
    @_checkDependencies()
    @domainRepository.createNewUid callback

  @_onEvent: (eventName, callback) =>
    @eventHandlers[eventName] ?= []
    @eventHandlers[eventName].push callback

  @_checkDependencies: =>
    throw new Error "Domain repository instance not set on constructor" unless @domainRepository?
    throw new Error "Command bus instance not set on constructor" unless @commandBus?

module.exports = Entity
