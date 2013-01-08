var inherit                = require("../../inherit");
var CommonEventBusReceiver = require("../common/receiver");
var InMemoryEventBus;

inherit(InMemoryEventBusReceiver, CommonEventBusReceiver);

function InMemoryEventBusReceiver(options) {
  var self = this;
  self._super(options);

  InMemoryEventBus = InMemoryEventBus || require("../in_memory");
}

InMemoryEventBusReceiver.prototype.start = function (callback) {
  var self = this;

  self.queue = InMemoryEventBus.registerQueue({
    name: self.queueName,
    logger: self.logger
  });

  self.queue.registerHandler(function (event, callback) {
    self._handleEvent(event, callback);
  });

  callback();
};

module.exports = InMemoryEventBusReceiver;
