async = require "async"

COUCHDB_STORE = "couchdb"
REDIS_STORE   = "redis"

class DomainRepository

  @aggregates    = []
  @eventHandlers = {}

  @initializeStoreWithConfiguration: (configuration) =>
    switch configuration.domain.store
      when COUCHDB_STORE
        CouchDbEventStore = require "./event_store/couchdb"
        @store = new CouchDbEventStore configuration.domain.uri
      when REDIS_STORE
        RedisEventStore = require "./event_store/redis"
        @store = new RedisEventStore
      else
        throw new Error "Unknown domain repository store \"#{configuration.domain.store}\"."

  @setup: (callback) =>
    @store.setup callback

  @transact: (operation, callback) =>
    operation (err) =>
      if err
        @rollback callback
      else
        @commit callback

  @createNewUid: (callback) =>
    @store.createNewUid callback

  @findAggregateByUid: (entityConstructor, uid, options, callback) =>
    [options, callback] = [{}, options] unless callback?

    @store.findAllByAggregateUid uid, (err, events) ->
      if err?
        callback err
      else
        entityConstructor.buildFromEvents events, callback

  @replayAllEvents: (callback) =>
    @store.findAll loadBlobs: true, (err, events) =>
      if events.length > 0
        eventQueue = async.queue (event, eventTaskCallback) =>
          @publishEvent event, eventTaskCallback
        , 1
        eventQueue.drain = callback
        eventQueue.push events
      else
        callback()

  @add: (aggregate) =>
    @aggregates.push aggregate

  @commit: (callback) =>
    if @aggregates.length is 0
      callback null
      return

    aggregateQueue = async.queue (aggregate, aggregateTaskCallback) =>
      firstEvent = aggregate.appliedEvents.shift()

      if firstEvent?
        queue = async.queue (event, eventTaskCallback) =>
          nextEvent = aggregate.appliedEvents.shift()
          queue.push nextEvent if nextEvent?

          @store.saveEvent event, (err, event) =>
            if err?
              eventTaskCallback err
            else
              @publishEvent event, eventTaskCallback
        , 1

        queue.drain = aggregateTaskCallback
        queue.push [firstEvent]
      else
        aggregateTaskCallback null

    , Infinity # TODO determine if it is safe to treat all aggregates in parallel?

    aggregateQueue.drain = callback # todo: empty @aggregates to free up memory?
                                    # todo: handle errors from child queues with Q.all()
    aggregateQueue.push @aggregates

  @rollback: (callback) =>
    @aggregates.forEach (aggregate) =>
      aggregate.appliedEvents = []
    callback()

  @onEvent: (eventName, eventHandler) =>
    @eventHandlers[eventName] ?= []
    @eventHandlers[eventName].push eventHandler

  @publishEvent: (event, callback) =>
    eventHandlers = @eventHandlers[event.name] or []
    if eventHandlers.length is 0
      callback null
      return

    queue = async.queue (eventHandler, eventHandlerTaskCallback) =>
      eventHandler event, (err) ->
        if err?
          eventHandlerTaskCallback err
        else
          eventHandlerTaskCallback null
    , 1

    queue.drain = callback
    queue.push eventHandlers

module.exports = DomainRepository
