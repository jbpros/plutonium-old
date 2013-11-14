Entity  = require "./entity"
inherit = require "./inherit"

AggregateRoot = (name, finalCtor, Ctor) ->

  unless finalCtor?
    finalCtor = ->
      @constructor.super_.apply @, arguments

  Ctor = finalCtor unless Ctor?

  Base = Entity name, finalCtor, ->
    Base.super_.apply this, arguments
    @

  Base::toString = ->
    "[object AggregateRoot:#{@constructor.name} <uid: #{@uid}>]"

  inherit Ctor, Base
  Ctor

AggregateRoot.initialize = (options) ->
  throw new Error "Missing logger" unless options.logger?
  AggregateRoot.logger = options.logger

module.exports = AggregateRoot
