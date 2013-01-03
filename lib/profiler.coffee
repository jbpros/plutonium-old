microtime = require "microtime"

class Profiler
  constructor: (@subject, @logger) ->
    throw new Error "Missing logger" unless @logger

  start: (subject) ->
    this.startTime = microtime.now()

  end: ({silent} = {}) ->
    end = microtime.now()
    @spent = end - @startTime
    unless silent
      spent = "#{_padLeft @spent, 10}µs"
      @logger.debug "profiler", "#{spent} on #{@subject}"


_padLeft = (str, length) ->
  str = str.toString()
  while str.length < length
    str = " " + str
  return str

module.exports = Profiler
