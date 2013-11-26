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

  registerCommandHandler: (commandHandler) ->
    commandName = commandHandler.getCommandName()
    throw new Error "A command and its handler for command named \"#{command}\" were already registered" if @commandHandlers[commandName]?
    @commandHandlers[commandName] = commandHandler

  createNewUid: (callback) ->
    @domainRepository.createNewUid callback

  executeCommand: (command, callback) ->
    domainRepository = @domainRepository
    logger           = @logger
    @instantiateHandlerForCommand command, (err, commandHandler) ->
      return callback err if err?
      proceed = (callback) ->
        p = new Profiler "CommandBus#executeCommand(command execution)", logger
        p.start()
        commandHandler.run (args...) ->
          p.end()
          callback args...
      logger.log "CommandBus#executeCommand", "running command '#{command.getName()}'"
      domainRepository.transact proceed, callback

  deserializeCommand: (commandName, payload, callback) ->
    @getHandlerForCommandName commandName, (err, CommandHandler) ->
      return callback err if err?
      Command = CommandHandler.getCommand()
      command = new Command payload
      callback null, command

  instantiateHandlerForCommand: (command, callback) ->
    commandName = command.getName()
    @getHandlerForCommandName commandName, (err, CommandHandler) ->
      return callback err if err?
      commandHandler = new CommandHandler command
      callback null, commandHandler

  getHandlerForCommandName: (commandName, callback) ->
    CommandHandler = @commandHandlers[commandName]
    if not CommandHandler?
      callback new Error "No handler for command \"#{commandName}\" was found"
    else
      callback null, CommandHandler

  close: (callback) ->
    @server.close callback

module.exports = CommandBus