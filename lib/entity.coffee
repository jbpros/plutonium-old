async    = require "async"
Event    = require "./event"
Profiler = require "./profiler"

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
    logger = @logger

    @constructor._checkDependencies()
    eventHandlers = Entity.eventHandlers[event.name] || []
    return callback null if eventHandlers.length is 0
    pending = 0
    errors = []
    for eventHandler in eventHandlers
      pending++
      eventHandler.call @, event, (err) ->
        if err?
          logger.error "Entity#applyEvent", "a handler failed: #{err}"
          errors.push err
        pending--
        if pending is 0
          error = if errors.length > 0 then new Error "Some event handler(s) failed" else null
          callback error

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
    p = new Profiler "Entity.buildFromEvents(#{events.length} events)", Entity.logger
    _t = 0
    i = 0
    p.start()
    processNextEvent = =>
      event = events[i++]
      if event?
        p1 = new Profiler "Entity.buildFromEvents(single event)", Entity.logger
        p1.start()
        entity.applyEvent event, (err) ->
          p1.end(silent: true)
          _t += p1.spent
          return callback err if err?
          process.nextTick processNextEvent
      else
        p.end()
        Entity.logger.debug "profiler", "spent #{_t}µs playing events"
        callback null, entity
    processNextEvent()

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
