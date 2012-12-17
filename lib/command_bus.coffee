DomainRepository = require "./domain_repository"

class CommandBus

  constructor: (@domainRepository) ->
    throw new Error "Missing domain repository" unless @domainRepository?
    @commandHandlers = {}

  registerCommandHandler: (command, handler) ->
    @commandHandlers[command] = handler

  executeCommand: (commandName, args..., callback) ->
    domainRepository = @domainRepository
    @getHandlerForCommand commandName, (err, commandHandler) ->
      return callback err if err?

      proceed = (callback) ->
        args.push callback
        commandHandler.apply null, args
      domainRepository.transact proceed, callback

  getHandlerForCommand: (commandName, callback) ->
    commandHandler = @commandHandlers[commandName]
    if not commandHandler?
      callback new Error "No handler for command \"#{commandName}\" was found"
    else
      callback null, commandHandler

module.exports = CommandBus
