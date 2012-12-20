var InMemoryEventBus;

function InMemoryEventBusEmitter(configuration) {
  InMemoryEventBus = InMemoryEventBus || require("../in_memory");
}

InMemoryEventBusEmitter.prototype.emit = function (event, callback) {
  InMemoryEventBus.broadcastEvent(event, callback);
};

module.exports = InMemoryEventBusEmitter;