Profiler           = require "./profiler"
EntityInstantiator = require "./entity_instantiator"
util               = require "util"
Queue              = require "./queue"

class DomainRepository

  constructor: ({@store, @emitter, @logger, @replaying}) ->
    throw new Error "Missing store" unless @store?
    throw new Error "Missing event bus emitter" unless @emitter?
    throw new Error "Missing logger" unless @logger?

    @entityEvents          = {}
    @directListeners       = {}
    @nextDirectListenerKey = 0
    @transacting           = false
    @halted                = false
    @silent                = false
    @transactionQueue      = new Queue (transaction, done) =>
      if @halted
        @logger.warning "transaction", "skipped (#{@transactionQueue.length()} more transaction(s) in queue)"
        transaction.callback() if transaction.callback?
        done()
      else
        @transacting = true
        @logger.log "transaction", "starting"
        p = new Profiler "DomainRepository(transactionQueue.operation)", @logger
        p.start()
        transaction (err) =>
          p.end()
          if err?
            @logger.alert "transaction", "failed, rolling back (#{err.stack || util.inspect(err)})"
            @_rollback =>
              @logger.log "transaction", "rolled back (#{@transactionQueue.length()} more transaction(s) in queue)"
              @transacting = false
              done()
          else
            @logger.log "transaction", "succeeded, comitting"
            @_commit (err) =>
              throw err if err?
              @logger.log "transaction", "committed (#{@transactionQueue.length()} more transaction(s) in queue)"
              @transacting = false
              done()
    , 1

  queueTransaction: (transaction) ->
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

  replayAllEvents: (options, callback) ->
    return callback new Error("Replay mode not set") unless @replaying

    [options, callback] = [{}, options] unless callback?
    lastEvent           = null

    @store.iterateOverAllEvents options, (event, eventHandlerCallback) =>
      event.replayed = true
      lastEvent      = event
      @_publishEvent event, eventHandlerCallback
    , (err) ->
      return callback err if err?
      callback null, lastEvent

  getLastPublishedEvents: () ->
    @emitter.lastEmittedEvents

  findAllEventsByEntityUid: (entityUid, callback) ->
    @store.findAllEventsByEntityUid entityUid, callback

  add: (entity) ->
    throw new Error "Cannot write during replay" if @replaying
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

    entityQueue = new Queue (entityAppliedEvents, entityTaskCallback) =>
      firstEvent = entityAppliedEvents.shift()

      if firstEvent?
        queue = new Queue (event, eventTaskCallback) =>
          nextEvent = entityAppliedEvents.shift()
          queue.push nextEvent if nextEvent?

          @logger.log "commit", "saving event \"#{event.name}\" for entity #{event.entityUid}"
          @store.saveEvent event, (err, event) ->
            return callback err if err?
            savedEvents.push event
            eventTaskCallback null
        , 1

        queue.drain = entityTaskCallback
        queue.push [firstEvent]
      else
        entityTaskCallback null

    , Infinity # TODO determine if it is safe to treat all entitys in parallel?

    entityQueue.drain = =>
      return callback null unless savedEvents.length > 0

      publicationQueue = new Queue (event, publicationCallback) =>
        @_publishEvent event, (err) ->
          return callback err if err?
          publicationCallback()
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
    @_publishEventToDirectListeners event, (err) =>
      return callback err if err?
      @_publishEventToEventBus event, callback

  _publishEventToEventBus: (event, callback) ->
    if @silent
      callback null
    else
      @emitter.emit event, callback

  _publishEventToDirectListeners: (event, callback) ->
    directListeners  = @directListeners[event.name]
    pending          = 0
    errors           = []
    queuedListeners  = false
    logger           = @logger

    for _, directListener of directListeners
      if !queuedListeners
        queuedListeners = true
        logger.log "DomainRepository#_publishEventToDirectListeners", "publishing \"#{event.name}\" from entity #{event.entityUid} to direct listeners"

      pending++
      directListener event, (err) ->
        if err?
          logger.error "DomainRepository#_publishEventToDirectListeners", "a direct listener failed: #{err}"
          errors.push err
        pending--
        if pending is 0
          error = if errors.length > 0 then new Error "Some direct listener(s) failed" else null
          callback error

    callback null unless queuedListeners

module.exports = DomainRepository
