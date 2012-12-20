async = require "async"

COUCHDB_STORE = "couchdb"
REDIS_STORE   = "redis"

class DomainRepository

  constructor: ({@store, @emitter, @logger}) ->
    throw new Error "Missing store" unless @store?
    throw new Error "Missing event bus emitter" unless @emitter?
    throw new Error "Missing logger" unless @logger?
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
      @emitter.emit event, (err) =>
        if err?
          @logger.log "publishEvent", "event publication failed: #{err}"
        callback err

module.exports = DomainRepository
