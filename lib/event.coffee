class Event

  constructor: ({@name, @data, @uid, @aggregateUid, @timestamp}) ->
    throw new Error "Missing name" unless @name
    throw new Error "Missing data" unless @data

    @timestamp ?= Date.now()

module.exports = Event
