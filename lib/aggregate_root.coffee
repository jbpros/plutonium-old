Entity = require "./entity"

class AggregateRoot extends Entity

  constructor: ->
    super

  toString: ->
    "[object AggregateRoot:#{@constructor.name} <uid: #{@uid}>]"

module.exports = AggregateRoot
