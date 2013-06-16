class Profiler
  constructor: (@subject, @logger) ->
    throw new Error "Missing logger" unless @logger

  start: (subject) ->
    this.startTime = process.hrtime()

  end: ({silent} = {}) ->
    diff = process.hrtime(this.startTime)
    @spent = (diff[0] * 1e9 + diff[1]) / 1000000
    unless silent
      spent = "#{_padLeft @spent, 13}ms"
      @logger.debug "profiler", "#{spent} on #{@subject}"

_padLeft = (str, length) ->
  str = str.toString()
  while str.length < length
    str = " " + str
  return str

module.exports = Profiler
