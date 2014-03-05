function Reporter(attributes) {
  if (!attributes.app)              throw new Error("Missing app");
  if (!attributes.logger)           throw new Error("Missing logger");
  if (!attributes.eventBusReceiver) throw new Error("Missing event bus receiver");
  this.app              = attributes.app;
  this.logger           = attributes.logger;
  this.eventBusReceiver = attributes.eventBusReceiver;
}

Reporter.prototype.destroy = function(callback) {
  this.logger           = null;
  this.eventBusReceiver = null;
  callback();
};

Reporter.prototype.getQueueName = function() {
  return this.eventBusReceiver.queueName;
};

module.exports = Reporter;