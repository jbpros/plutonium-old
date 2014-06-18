class BaseEventStore

  setup: (callback) =>
    throw new Error "Implement me"

  createNewUid: (callback) =>
    throw new Error "Implement me"

  findAllEvents: (options, callback) ->
    throw new Error "implement me"

  iterateOverAllEvents: (options, eventHandler, callback) ->
    throw new Error "implement me"

  findAllEventsByEntityUid: (entityUid, options, callback) ->
    throw new Error "Implement me"

  loadSnapshotForEntityUid: (uid, callback) ->
    # implement me if you want snapshots in your store
    callback null, null

  saveEvent: (event, callback) =>
    throw new Error "Implement me"

  saveSnapshot: (snapshot, callback) =>
    # implement me if you want snapshots in your store
    callback? null

module.exports = BaseEventStore
