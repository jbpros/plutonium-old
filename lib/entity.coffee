async        = require "async"
DomainObject = require "./domain_object"
inherit      = require "./inherit"

Entity = (name, finalCtor, Ctor) ->

  unless finalCtor?
    finalCtor = ->
      @constructor.super_.apply @, arguments

  Ctor = finalCtor unless Ctor?

  Base = DomainObject name, finalCtor, ->
    Base.super_.apply this, arguments
    @uid     = null
    @$logger = finalCtor.$logger
    @

  Base::toString = ->
    "[object Entity:#{@constructor.name}]"

  Base.initialize = ->
    @$logger = Entity.logger

  inherit Ctor, Base
  Ctor

Entity.initialize = (options) ->
  throw new Error "Missing logger" unless options.logger?
  Entity.logger = options.logger

module.exports = Entity