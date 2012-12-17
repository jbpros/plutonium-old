DomainRepository = require "./domain_repository"

commandHandlers = {}

class CommandBus

  @registerCommandHandler: (command, handler) =>
    commandHandlers[command] = handler

  @executeCommand: (commandName, args..., callback) ->
    @getHandlerForCommand commandName, (err, commandHandler) ->
      return callback err if err?

      proceed = (callback) ->
        args.push callback
        commandHandler.apply null, args
      DomainRepository.transact proceed, callback

  @getHandlerForCommand: (commandName, callback) ->
    commandHandler = commandHandlers[commandName]
    if not commandHandler?
      callback new Error "No handler for command \"#{commandName}\" was found"
    else
      callback null, commandHandler

module.exports = CommandBus
