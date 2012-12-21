var InMemoryEventBus;

function InMemoryEventBusEmitter(configuration) {
  InMemoryEventBus = InMemoryEventBus || require("../in_memory");
}

InMemoryEventBusEmitter.prototype.emit = function (event, callback) {
  InMemoryEventBus.broadcastEvent(event, callback);
};

InMemoryEventBusEmitter.prototype.clearQueues = function (callback) {
  InMemoryEventBus.clearQueues(callback);
};

module.exports = InMemoryEventBusEmitter;
