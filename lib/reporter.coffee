class Reporter

  constructor: ({@eventBusReceiver, @logger}) ->
    throw new Error "Missing event bus receiver" unless @eventBusReceiver?
    throw new Error "Missing logger" unless @logger?

module.exports = Reporter
