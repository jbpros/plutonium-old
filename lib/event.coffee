class Event
  constructor: (@name, @data) ->
    throw new Error "Missing name" unless @name
    throw new Error "Missing data" unless @data

module.exports = Event
