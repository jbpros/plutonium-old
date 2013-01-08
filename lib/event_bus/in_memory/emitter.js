var inherit               = require("../../inherit");
var CommonEventBusEmitter = require("../common/emitter");
var InMemoryEventBus;

inherit(InMemoryEventBusEmitter, CommonEventBusEmitter);

function InMemoryEventBusEmitter(options) {
  InMemoryEventBus = InMemoryEventBus || require("../in_memory");

  options = options || {};
  if (!options.logger)
    throw new Error("Missing logger");

  this.logger = options.logger;
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
