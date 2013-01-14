class Report
  constructor: (@attributes) ->

  get: (attributeName) ->
    @attributes[attributeName]

  getAttributes: ->
    @attributes

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

module.exports = Report
