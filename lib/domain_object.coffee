stream = require "stream"
inherit = require "./inherit"

HIDDEN_PROPERTY_NAME_REGEXP = /(^\$)|(^constructor$)/

DomainObject = (name, finalCtor, Ctor) ->

  throw new Error "Missing final constructor" unless finalCtor?
  throw new Error "Missing constructor" unless Ctor?

  Base = ->

  Base::toString = ->
    "[object DomainObject:#{name}]"

  Base::super = ->
    @constructor.super_.apply @, arguments

  Base::_serialize = ->
    properties = {}
    serialized =
      name: name
      properties: properties

    for property, member of @
      continue unless @hasOwnProperty property
      continue if property.match HIDDEN_PROPERTY_NAME_REGEXP
      properties[property] = serialize member

    serialized

  DomainObject.register name, finalCtor
  inherit Ctor, Base
  Ctor

DomainObject.constructors = {}

DomainObject.initialize = (options) ->
  throw new Error "Missing logger" unless options.logger?
  DomainObject.logger = options.logger

DomainObject.initializeConstructors = ->
  for name, ctor of DomainObject.constructors
    DomainObject.logger.log "DomainObject", "initialize domain object \"#{name}\""
    ctor.initialize()

DomainObject.register = (name, Ctor) ->
  throw new Error "#{name} is already registered" if DomainObject.getByName(name)?
  DomainObject.constructors[name] = Ctor

DomainObject.getByName = (name) ->
  DomainObject.constructors[name]

DomainObject.deserialize = (obj) ->
  deserializeDomainObject obj

serialize = (obj) ->
  if obj is null
    null
  else if obj is undefined
    undefined
  else if obj._serialize?
    obj._serialize true
  else if obj instanceof Array
    serializeArray obj
  else if typeof obj is "object"
    serializeObject obj
  else if typeof obj is "function"
    undefined
  else # string, number, undefined
    obj

serializeArray = (array) ->
  serialized = []
  for item in array
    serializedItem = serialize item
    serialized.push item
  serialized

serializeObject = (obj) ->
  throw new Error "Streams cannot be serialized" if obj instanceof stream.Stream
  return obj if Buffer.isBuffer obj

  properties = {}
  serialized = name: null, properties: properties
  for property, member of obj
    continue unless obj.hasOwnProperty property
    properties[property] = serialize member
  serialized

deserializeDomainObject = (obj) ->
  Ctor = DomainObject.getByName obj.name
  deserialized = {}
  deserialized.constructor = Ctor
  deserialized.__proto__ = Ctor.prototype
  for property, value of Object.keys Ctor
    deserialized[property] = value if Ctor.hasOwnProperty property
  for property, value of obj.properties
    deserialized[property] = deserialize value
  deserialized.$__deserialized__ = true
  deserialized

deserialize = (obj) ->
  if obj is null
    null
  else if obj instanceof Array
    deserializeArray obj
  else if typeof obj == "object"
    deserializeObject obj
  else
    obj

deserializeObject = (obj) ->
  if Buffer.isBuffer obj
    obj
  else if obj.name?
    deserializeDomainObject obj
  else
    deserialized = {}
    for property, value of obj.properties
      deserialized[property] = deserialize value
    deserialized

deserializeArray = (array) ->
  deserialized = []
  for item in array
    deserializedItem = deserialize item
    deserialized.push item
  deserialized

module.exports = DomainObject