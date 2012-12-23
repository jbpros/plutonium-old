async = require "async"

COUCHDB_STORE = "couchdb"
REDIS_STORE   = "redis"

class DomainRepository

  constructor: ({@store, @emitter, @logger}) ->
    throw new Error "Missing store" unless @store?
    throw new Error "Missing event bus emitter" unless @emitter?
    throw new Error "Missing logger" unless @logger?
    @aggregates = []
    @directListeners = {}

  transact: (operation, callback) ->
    @logger.log "transaction", "starting"
    operation (err) =>
      if err?
        @logger.alert "transaction", "rolling back (#{err})"
        @rollback callback
      else
        @logger.log "transaction", "comitting"
        @commit callback

  createNewUid: (callback) ->
    @store.createNewUid callback

  findAggregateByUid: (entityConstructor, uid, options, callback) ->
    [options, callback] = [{}, options] unless callback?

    @store.findAllByAggregateUid uid, (err, events) ->
      if err?
        callback err
      else
        entityConstructor.buildFromEvents events, callback

  replayAllEvents: (callback) ->
    @store.findAll loadBlobs: true, (err, events) =>
      if events.length > 0
        eventQueue = async.queue (event, eventTaskCallback) =>
          @publishEvent event, eventTaskCallback
        , 1
        eventQueue.drain = callback
        eventQueue.push events
      else
        callback()

  add: (aggregate) ->
    @aggregates.push aggregate

  commit: (callback) ->
    return callback nill if @aggregates.length is 0

    committedEvents = []

    aggregateQueue = async.queue (aggregate, aggregateTaskCallback) =>
      firstEvent = aggregate.appliedEvents.shift()

      if firstEvent?
        queue = async.queue (event, eventTaskCallback) =>
          nextEvent = aggregate.appliedEvents.shift()
          queue.push nextEvent if nextEvent?

          @logger.log "commit", "saving event \"#{event.name}\" for aggregate #{event.aggregateUid}"
          @store.saveEvent event, (err, event) =>
            if err?
              eventTaskCallback err
            else
              committedEvents.push event
              eventTaskCallback null
        , 1

        queue.drain = aggregateTaskCallback
        queue.push [firstEvent]
      else
        aggregateTaskCallback null

    , Infinity # TODO determine if it is safe to treat all aggregates in parallel?

    aggregateQueue.drain = (err) =>
      return callback err if err?
      return callback null unless committedEvents.length > 0
      publicationQueue = async.queue (event, publicationCallback) =>
        @publishEvent event, publicationCallback
      , Infinity
      publicationQueue.drain = callback
      publicationQueue.push committedEvents

    aggregateQueue.push @aggregates
    @aggregates = []

  rollback: (callback) ->
    @aggregates.forEach (aggregate) =>
      aggregate.appliedEvents = []
    callback()

  publishEvent: (event, callback) ->
    @logger.log "publishEvent", "#{event.name} for aggregate #{event.aggregateUid}"
    process.nextTick =>
      @_publishEventToDirectListeners event, (err) =>
        @logger.warn "publishEvent", "a direct listener failed: #{err}" if err?
        @emitter.emit event, (err) =>
          @logger.log "publishEvent", "event publication failed: #{err}" if err?
          callback err

  # Listen for events before they are published to the event bus
  # Don't use this in reporters/reports! This is reserved for domain routines
  #
  # Options:
  # * synchronous: Boolean - the event listener is considered synchronous, it will not receive any callbacks
  # * once: Boolean - automatically unregisters the listener after one execution
  # * forAggregateUid: String - the listener is executed only if the aggregate UID matches the given string
  onEvent: (eventName, options, listener) ->
    [eventName, options, listener] = [eventName, {}, options] if not listener?

    @directListeners[eventName] ?= []
    directListeners = @directListeners[eventName]
    directListenerIndex = directListeners.length

    if options.synchronous
      syncListener = listener
      listener = (event, callback) ->
        syncListener event
        callback()

    if options.once
      oneTimerListener = listener
      listener = (event, callback) ->
        directListeners.splice directListenerIndex, 1
        oneTimerListener event, callback

    if options.forAggregateUid?
      scopedListener = listener
      listener = (event, callback) ->
        if event.aggregateUid is options.forAggregateUid
          scopedListener event, callback
        else
          callback null

    directListeners.push listener

  _publishEventToDirectListeners: (event, callback) ->
    directListeners = @directListeners[event.name]
    return callback null unless directListeners and directListeners.length > 0
    queue = async.queue (directListener, callback) ->
      directListener event, callback
    , Infinity

    queue.drain = callback
    queue.push directListeners

module.exports = DomainRepository
