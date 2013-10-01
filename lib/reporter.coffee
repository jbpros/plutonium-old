class Reporter

  constructor: ({@eventBusReceiver, @logger, @app}) ->
    throw new Error "Missing event bus receiver" unless @eventBusReceiver?
    throw new Error "Missing logger" unless @logger?
    throw new Error "Missing app" unless @app?

  destroy: (callback) ->
    @eventBusReceiver = null
    @logger = null
    callback()

module.exports = Reporter
