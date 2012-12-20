class Report
  constructor: (@attributes) ->

  get: (attributeName) ->
    @attributes[attributeName]

  toJSON: (options = {}) ->
    attributes = {}
    for k, v of @attributes
      attributes[k] = v unless v instanceof Buffer
    attributes

  inspect: ->
    @toString()

  toString: ->
    attrs = []
    for k, v of @attributes
      attrs.push "#{k}=\"#{v}\"" unless v instanceof Buffer
    "[reportObject #{@.constructor.name} <#{attrs.join ', '}>]"

  @initialize: ({@eventBusReceiver, @logger}) ->
    throw new Error "Missing event bus receiver" unless @eventBusReceiver?
    throw new Error "Missing logger" unless @logger?

module.exports = Report
