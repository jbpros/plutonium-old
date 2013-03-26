class Event

  constructor: ({@name, @data, @uid, @aggregateUid, @timestamp, @version, @replayed}) ->
    throw new Error "Missing name" unless @name
    throw new Error "Missing data" unless @data

    @timestamp ?= Date.now()

module.exports = Event
