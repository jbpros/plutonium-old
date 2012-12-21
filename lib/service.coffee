class Service

  constructor: ->
    @constructor._checkDependencies()
    @domainRepository = Service.domainRepository
    @commandBus       = Service.commandBus
    @logger           = Service.logger

  @initialize: (options) =>
    throw new Error "Missing domain repository" unless options.domainRepository?
    throw new Error "Missing command bus" unless options.commandBus?
    throw new Error "Missing logger" unless options.logger?
    Service.domainRepository = options.domainRepository
    Service.commandBus = options.commandBus
    Service.logger     = options.logger

  @_checkDependencies: =>
    throw new Error "Command bus instance not set on constructor" unless @commandBus?
    throw new Error "Logger instance not set on constructor" unless @logger?

module.exports = Service
