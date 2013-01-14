dnode = require "dnode"

class CommandBusClient

  constructor: ({@port, @logger}) ->
    throw new Error "Missing port" unless @port?
    throw new Error "Missing logger" unless @logger?

  createNewUid: (callback) ->
    logger = @logger
    logger.log "CommandBusClient", "creating UID from localhost:#{@port}..."
    client = dnode.connect @port
    client.on "remote", (remote) ->
      remote.createNewUid (err, uid) ->
        if (err)
          logger.error "CommandBusClient", "failed to create UID: #{err}"
        else
          logger.log "CommandBusClient", "UID created: #{uid}"
        callback err, uid

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