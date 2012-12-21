var InMemoryEventBus;

function InMemoryEventBusEmitter(configuration) {
  InMemoryEventBus = InMemoryEventBus || require("../in_memory");
}

InMemoryEventBusEmitter.prototype.emit = function (event, callback) {
  InMemoryEventBus = InMemoryEventBus || require("../in_memory");
  InMemoryEventBus.broadcastEvent(event, callback);
};

InMemoryEventBusEmitter.prototype.clearQueues = function (callback) {
  InMemoryEventBus = InMemoryEventBus || require("../in_memory");
  InMemoryEventBus.clearQueues(callback);
};

module.exports = InMemoryEventBusEmitter;
