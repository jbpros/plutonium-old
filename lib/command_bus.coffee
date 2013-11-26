DomainRepository = require "./domain_repository"
CommandBusServer = require "./command_bus_server"
Profiler         = require "./profiler"

class CommandBus

  constructor: ({@domainRepository, @logger, @port}) ->
    throw new Error "Missing domain repository" unless @domainRepository?
    throw new Error "Missing logger" unless @logger?

    if @port
      @server = new CommandBusServer commandBus: @, port: @port, logger: @logger
      @server.listen @port

    @commandHandlers = {}

  registerCommandHandler: (command, handler) ->
    throw new Error "A cmmand named \"#{command}\" is already registered" if @commandHandlers[command]?
    @commandHandlers[command] = handler

  createNewUid: (callback) ->
    @domainRepository.createNewUid callback

  executeCommand: (commandName, payload, callback) ->
    domainRepository = @domainRepository
    logger           = @logger
    @getHandlerForCommand commandName, (err, commandHandler) ->
      return callback err if err?
      proceed = (callback) ->
        p = new Profiler "CommandBus#executeCommand(command execution)", logger
        p.start()
        commandHandler payload, (args...) ->
          p.end()
          callback args...
      logger.log "CommandBus#executeCommand", "running command '#{commandName}'"
      domainRepository.transact proceed, callback

  getHandlerForCommand: (commandName, callback) ->
    commandHandler = @commandHandlers[commandName]
    if not commandHandler?
      callback new Error "No handler for command \"#{commandName}\" was found"
    else
      callback null, commandHandler

  close: (callback) ->
    @server.close callback

module.exports = CommandBus
