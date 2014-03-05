async        = require "async"
Q            = require "q"
DomainObject = require "./domain_object"
inherit      = require "./inherit"
defer        = require "./defer"
Profiler     = require "./profiler"
Event        = require "./event"

eventHandlers = {}

Entity = (name, finalCtor, Ctor) ->

  unless finalCtor?
    finalCtor = ->
      @constructor.super_.apply @, arguments

  Ctor = finalCtor unless Ctor?

  Base = DomainObject name, finalCtor, ->
    Base.super_.apply this, arguments
    @uid     = null
    @_initializeAtVersion 0
    @

  Base.eventHandlers = {} # inherited by final constructor

  Base::toString = ->
    "[object Entity:#{@constructor.name}]"

  Base.initialize = ->
    @$domainRepository = Entity.domainRepository
    @$commandBus       = Entity.commandBus
    @$logger           = Entity.logger

  Base::_initializeAtVersion = (version) ->
    @$version          = version
    @$appliedEvents    = []
    @$domainRepository = Entity.domainRepository
    @$commandBus       = Entity.commandBus
    @$logger           = Entity.logger

  Base::_serialize = (contained) ->
    throw new Error "References between entity are forbidden" if contained
    Base.super_.prototype._serialize.apply @, arguments

  Base::triggerEvent = (eventName, attributes, callback) ->
    event = new Event name: eventName, data: attributes
    @applyEvent event, (err) =>
      return callback err if err?
      event.entityUid = @uid
      @$appliedEvents.push event
      @$domainRepository.add @
      callback null

  Base::applyEvent = (event, callback) ->
    logger = @$logger

    @$version = event.version
    handlers = eventHandlers[event.name] || []
    return callback null if handlers.length is 0
    pending = 0
    errors = []
    for eventHandler in handlers
      pending++
      eventHandler.call @, event, (err) ->
        if err?
          logger.error "Entity#applyEvent", "a handler failed: #{err}"
          errors.push err
        pending--
        if pending is 0
          error = if errors.length > 0 then new Error "Some event handler(s) failed" else null
          callback error

  Base::applyEvents = (events, callback) ->
    p = new Profiler "Entity.applyEvents(#{events.length} events)", @$logger
    _t = 0
    i = 0
    p.start()
    processNextEvent = =>
      event = events[i++]
      if event?
        p1 = new Profiler "Entity.applyEvents(single event)", @$logger
        p1.start()
        @applyEvent event, (err) ->
          p1.end(silent: true)
          _t += p1.spent
          return callback err if err?
          defer processNextEvent
      else
        p.end()
        @$logger.debug "profiler", "spent #{_t}µs applying events"
        callback null, @
    defer processNextEvent

  Base.findByUid = (uid, callback) ->
    deferred = Q.defer()
    deferred.promise.nodeify callback
    if uid?
      @$domainRepository.findEntityByUid @, uid, (err, entity) ->
        if err?
          deferred.reject err
        else if not entity
          deferred.reject new Error "Could not find entity with UID " + uid
        else
          deferred.resolve entity
    else
      deferred.reject(new Error 'Please provide a UID to find')
    deferred.promise

  Base.findAllEvents = (uid, callback) ->
    @$domainRepository.findAllEventsByEntityUid uid, callback

  Base.buildFromEvents = (events, callback) ->
    entity = new @
    entity.applyEvents events, callback

  Base.createNewUid = (callback) ->
    deferred = Q.defer()
    deferred.promise.nodeify callback
    @$domainRepository.createNewUid deferred.makeNodeResolver()
    deferred.promise

  Base._onEvent = (eventName, callback) ->
    eventHandlers[eventName] ?= []
    eventHandlers[eventName].push callback

  inherit Ctor, Base
  Ctor

Entity.initialize = (options) ->
  throw new Error "Missing domain repository" unless options.domainRepository?
  throw new Error "Missing command bus" unless options.commandBus?
  throw new Error "Missing logger" unless options.logger?
  Entity.domainRepository = options.domainRepository
  Entity.commandBus = options.commandBus
  Entity.logger = options.logger

module.exports = Entity
