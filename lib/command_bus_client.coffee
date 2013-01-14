dnode = require "dnode"

class CommandBusClient

  constructor: ({@port, @logger}) ->
    throw new Error "Missing port" unless @port?
    throw new Error "Missing logger" unless @logger?

  executeCommand: (commandName, args..., callback) ->
    logger = @logger
    logger.log "CommandBusClient", "sending command \"#{commandName}\" to localhost:#{@port}..."
    client = dnode.connect @port
    client.on "remote", (remote) ->
      remote.executeCommand commandName, args..., (err) ->
        if (err)
          logger.error "CommandBusClient", "command failed remotely: #{err}"
        else
          logger.log "CommandBusClient", "command sent"
        callback err

module.exports = CommandBusClient