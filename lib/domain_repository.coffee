async              = require "async"
Profiler           = require "./profiler"
EntityInstantiator = require "./entity_instantiator"
util               = require "util"
defer              = require "./defer"

COUCHDB_STORE = "couchdb"
REDIS_STORE   = "redis"

class DomainRepository

  constructor: ({@store, @emitter, @logger}) ->
    throw new Error "Missing store" unless @store?
    throw new Error "Missing event bus emitter" unless @emitter?
    throw new Error "Missing logger" unless @logger?
    @entityEvents          = {}
    @directListeners       = {}
    @nextDirectListenerKey = 0
    @transacting           = false
    @halted                = false
    @silent                = false
    @transactionQueue      = async.queue (transaction, done) =>
      if @halted
        @logger.warning "transaction", "skipped (#{@transactionQueue.length()} more transaction(s) in queue)"
        done()
      else
        @transacting = true
        @logger.log "transaction", "starting"
        p = new Profiler "DomainRepository(transactionQueue.operation)", @logger
        p.start()
        transaction (err) =>
          p.end()
          if err?
            @logger.alert "transaction", "failed, rolling back (#{util.inspect(err)})"
            @_rollback =>
              @logger.log "transaction", "rolled back (#{@transactionQueue.length()} more transaction(s) in queue)"
              @transacting = false
              done()
          else
            @logger.log "transaction", "succeeded, comitting"
            @_commit =>
              @logger.log "transaction", "committed (#{@transactionQueue.length()} more transaction(s) in queue)"
              @transacting = false
              done()
    , 1

  transact: (transaction) ->
    @transactionQueue.push transaction
    @logger.log "transaction", "queued (queue size: #{@transactionQueue.length()})"

  halt: ({immediately}, callback) ->
    @halted = true
    @silent = true if immediately
    if @transacting || @transactionQueue.length() > 0
      @transactionQueue.drain = =>
        @transactionQueue.drain = null
        callback()
    else
      callback()

  resume: (callback) ->
    @halted = false
    @silent = false
    @transactionQueue.drain = null
    callback()

  createNewUid: (callback) ->
    @store.createNewUid callback

  findEntityByUid: (Entity, uid, options, callback) ->
    [options, callback] = [{}, options] unless callback?
    return callback new Error "Missing entity root constructor" unless Entity?
    return callback new Error "Missing UID" unless uid?
    entityInstantiator = new EntityInstantiator store: @store, Entity: Entity, logger: @logger
    entityInstantiator.findByUid uid, callback

  replayAllEvents: (callback) ->
    @store.findAllEvents (err, events) =>
      if events.length > 0
        eventQueue = async.queue (event, eventTaskCallback) =>
          event.replayed = true
          @_publishEvent event, eventTaskCallback
        , 1
        eventQueue.drain = callback
        eventQueue.push events
      else
        callback()

  getLastPublishedEvents: () ->
    @emitter.lastEmittedEvents

  findAllEventsByEntityUid: (entityUid, callback) ->
    @store.findAllEventsByEntityUid entityUid, callback

  add: (entity) ->
    throw new Error "Entity is missing its UID" unless entity.uid?
    @entityEvents[entity.uid] ?= []
    addedEvents = @entityEvents[entity.uid]
    for event in entity.$appliedEvents
      eventAdded = addedEvents.indexOf(event) isnt -1
      addedEvents.push event unless eventAdded # todo: use a Set instead of indexOf?

  # Listen for events before they are published to the event bus
  # Don't use this in reporters/reports! This is reserved for domain routines
  #
  # Options:
  # * synchronous: Boolean - the event listener is considered synchronous, it will not receive any callbacks
  # * once: Boolean - automatically unregisters the listener after one execution
  # * forEntityUid: String - the listener is executed only if the entity UID matches the given string
  onEvent: (eventName, options, listener) ->
    [eventName, options, listener] = [eventName, {}, options] if not listener?
    @directListeners[eventName] ?= {}
    directListeners = @directListeners[eventName]
    directListenerKey = @nextDirectListenerKey++

    if options.synchronous
      syncListener = listener
      listener = (event, callback) ->
        syncListener event
        callback()

    if options.once
      oneTimerListener = listener
      listener = (event, callback) ->
        delete directListeners[directListenerKey]
        oneTimerListener event, callback

    if options.forEntityUid?
      scopedListener = listener
      listener = (event, callback) ->
        if event.entityUid is options.forEntityUid
          scopedListener event, callback
        else
          callback null

    directListeners[directListenerKey] = listener

  _commit: (callback) ->
    return callback null if Object.keys(@entityEvents).length is 0

    savedEvents = [];

    entityQueue = async.queue (entityAppliedEvents, entityTaskCallback) =>
      firstEvent = entityAppliedEvents.shift()

      if firstEvent?
        queue = async.queue (event, eventTaskCallback) =>
          nextEvent = entityAppliedEvents.shift()
          queue.push nextEvent if nextEvent?

          @logger.log "commit", "saving event \"#{event.name}\" for entity #{event.entityUid}"
          @store.saveEvent event, (err, event) =>
            savedEvents.push(event);
            if err?
              eventTaskCallback err
            else
              eventTaskCallback null
        , 1

        queue.drain = entityTaskCallback
        queue.push [firstEvent]
      else
        entityTaskCallback null

    , Infinity # TODO determine if it is safe to treat all entitys in parallel?

    entityQueue.drain = (err) =>
      return callback err if err?
      return callback null unless savedEvents.length > 0
      publicationQueue = async.queue (event, publicationCallback) =>
        @_publishEvent event, publicationCallback
      , Infinity
      publicationQueue.drain = callback
      publicationQueue.push savedEvents

    for entityUid, entityEvents of @entityEvents
      entityQueue.push [entityEvents]
    @entityEvents = {}

  _rollback: (callback) ->
    @entityEvents = {}
    callback()

  _publishEvent: (event, callback) ->
    defer =>
      @logger.log "publishEvent", "publishing \"#{event.name}\" from entity #{event.entityUid} to direct listeners"
      @_publishEventToDirectListeners event, (err) =>
        @logger.warn "publishEvent", "a direct listener failed: #{err}" if err?
        @logger.log "publishEvent", "publishing \"#{event.name}\" from entity #{event.entityUid} to event bus"
        if @silent
          callback()
        else
          @emitter.emit event, (err) =>
            @logger.log "publishEvent", "event publication failed: #{err}" if err?
            callback err

  _publishEventToDirectListeners: (event, callback) ->
    directListeners = @directListeners[event.name]

    queue = async.queue (directListener, callback) ->
      directListener event, callback
    , Infinity

    queue.drain = callback
    queuedListeners = false
    for _, directListener of directListeners
      queuedListeners = true unless queuedListeners
      queue.push directListener

    callback null unless queuedListeners

module.exports = DomainRepository
