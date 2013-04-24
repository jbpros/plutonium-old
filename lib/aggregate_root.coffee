util     = require "util"
Entity   = require "./entity"
Event    = require "./event"
Profiler = require "./profiler"
inherit  = require "./inherit"
defer    = require "./defer"

eventHandlers = {}

AggregateRoot = (name, finalCtor, Ctor) ->

  unless finalCtor?
    finalCtor = ->
      @constructor.super_.apply @, arguments

  Ctor = finalCtor unless Ctor?

  Base = Entity name, finalCtor, ->
    Base.super_.apply this, arguments
    @uid               = null
    @_initializeAtVersion 0
    @

  Base.eventHandlers = {} # inherited by final constructor

  Base::toString = ->
    "[object AggregateRoot:#{@constructor.name} <uid: #{@uid}>]"

  Base::_initializeAtVersion = (version) ->
    @$version          = version
    @$appliedEvents    = []
    @$domainRepository = finalCtor.$domainRepository
    @$commandBus       = finalCtor.$commandBus
    @$logger           = finalCtor.$logger

  Base::_serialize = (contained) ->
    throw new Error "References between aggregate roots are forbidden" if contained
    Base.super_.prototype._serialize.apply @, arguments


  Base::triggerEvent = (eventName, attributes, callback) ->
    event = new Event name: eventName, data: attributes
    @applyEvent event, (err) =>
      return callback err if err?
      event.aggregateUid = @uid
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
    p = new Profiler "AggregateRoot.applyEvents(#{events.length} events)", @$logger
    _t = 0
    i = 0
    p.start()
    processNextEvent = =>
      event = events[i++]
      if event?
        p1 = new Profiler "AggregateRoot.applyEvents(single event)", @$logger
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

  Base.initialize = ->
    @$domainRepository = AggregateRoot.domainRepository
    @$commandBus       = AggregateRoot.commandBus
    @$logger           = AggregateRoot.logger

  Base.findByUid = (uid, callback) ->
    @$domainRepository.findAggregateByUid @, uid, callback

  Base.findAllEvents = (uid, callback) ->
    @$domainRepository.findAllEventsByAggregateUid uid, callback

  Base.buildFromEvents = (events, callback) ->
    entity = new @
    entity.applyEvents events, callback

  Base.createNewUid = (callback) ->
    @$domainRepository.createNewUid callback

  Base._onEvent = (eventName, callback) ->
    eventHandlers[eventName] ?= []
    eventHandlers[eventName].push callback

  inherit Ctor, Base
  Ctor

AggregateRoot.initialize = (options) ->
  throw new Error "Missing domain repository" unless options.domainRepository?
  throw new Error "Missing command bus" unless options.commandBus?
  throw new Error "Missing logger" unless options.logger?
  AggregateRoot.domainRepository = options.domainRepository
  AggregateRoot.commandBus = options.commandBus
  AggregateRoot.logger = options.logger

module.exports = AggregateRoot