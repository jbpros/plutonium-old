async        = require "async"
EventEmitter = require("events").EventEmitter

COUCHDB_STORE = "couchdb"
REDIS_STORE   = "redis"

class DomainRepository

  constructor: (@store, @logger) ->
    throw new Error "Missing store" unless @store?
    thorw new Error "Missing logger" unless @logger?

    @emitter    = new EventEmitter
    @aggregates = []

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
          @publishEvent event
          eventTaskCallback()
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
      # todo: handle errors from child queues with Q.all()
      for event in committedEvents
        @publishEvent event
      callback()
    aggregateQueue.push @aggregates
    @aggregates = []

  rollback: (callback) ->
    @aggregates.forEach (aggregate) =>
      aggregate.appliedEvents = []
    callback()

  onEvent: (eventName, options, listener) ->
    [options, listener] = [{}, options] unless listener?

    register = "on"

    if options.forAggregateUid?
      innerListener = listener
      listener = (event) =>
        if event.aggregateUid is options.forAggregateUid
          @emitter.removeListener eventName, listener if options.once
          innerListener event
    else if options.once
      register = "once"

    listenerWithCallback = (event) =>
      listener event, =>
        @logger.log "event bus", "listener duty finished"

    @emitter[register].call @emitter, eventName, listenerWithCallback

  publishEvent: (event) ->
    @logger.log "publishEvent", "#{event.name} for aggregate #{event.aggregateUid}"
    process.nextTick =>
      @emitter.emit event.name, event

module.exports = DomainRepository
