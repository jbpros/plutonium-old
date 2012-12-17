class Service

  constructor: ->
    @constructor._checkCommandBusInstance()
    @commandBus = Service.commandBus

  @initialize: (commandBus) =>
    throw new Error "Missing command bus" unless commandBus?
    Service.commandBus = commandBus

  @_checkCommandBusInstance: =>
    throw new Error "Command bus instance not set on constructor" unless @commandBus?

module.exports = Service
