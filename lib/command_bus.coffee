dnode            = require "dnode"
DomainRepository = require "./domain_repository"
Profiler         = require "./profiler"

class CommandBus

  constructor: ({@domainRepository, @logger, @port}) ->
    throw new Error "Missing domain repository" unless @domainRepository?
    throw new Error "Missing logger" unless @logger?

    if @port
      server = dnode
        executeCommand: (commandName, args..., callback) =>
          @executeCommand commandName, args..., callback
      server.listen @port
      @logger.info "CommandBus", "listening on port #{@port}..."

    @commandHandlers = {}

  registerCommandHandler: (command, handler) ->
    @commandHandlers[command] = handler

  executeCommand: (commandName, args..., callback) ->
    domainRepository = @domainRepository
    logger           = @logger
    @getHandlerForCommand commandName, (err, commandHandler) ->
      return callback err if err?

      proceed = (callback) ->
        p = new Profiler "CommandBus#executeCommand(command execution)", logger
        args.push (args...) ->
          p.end()
          callback args...
        p.start()
        commandHandler.apply null, args
      domainRepository.transact proceed
      callback null

  getHandlerForCommand: (commandName, callback) ->
    commandHandler = @commandHandlers[commandName]
    if not commandHandler?
      callback new Error "No handler for command \"#{commandName}\" was found"
    else
      callback null, commandHandler

module.exports = CommandBus
