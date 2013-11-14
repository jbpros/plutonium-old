class Event

  constructor: ({@name, @data, @uid, @entityUid, @timestamp, @version, @replayed}) ->
    throw new Error "Missing name" unless @name
    throw new Error "Missing data" unless @data

    @timestamp ?= Date.now()

module.exports = Event
