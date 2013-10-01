class Reporter

  constructor: ({@eventBusReceiver, @logger}) ->
    throw new Error "Missing event bus receiver" unless @eventBusReceiver?
    throw new Error "Missing logger" unless @logger?

  destroy: (callback) ->
    @eventBusReceiver = null
    @logger = null
    callback()

  getQueueName: () ->
  	@eventBusReceiver.queueName

module.exports = Reporter
