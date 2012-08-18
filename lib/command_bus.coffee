DomainRepository = require "./domain_repository"

commandHandlers = {}

class CommandBus

  @registerCommandHandler: (command, handler) =>
    commandHandlers[command] = handler

  @executeCommand: (command, expandable_args, callback) ->
    args     = Array.prototype.slice.apply arguments
    command  = args.shift()
    callback = args.pop()

    commandHandler = commandHandlers[command]
    if not commandHandler?
      callback new Error "No handler for command \"#{command}\" was found"
    else
      proceed = (callback) ->
        args.push callback
        commandHandler.apply null, args
      DomainRepository.transact proceed, callback

module.exports = CommandBus
