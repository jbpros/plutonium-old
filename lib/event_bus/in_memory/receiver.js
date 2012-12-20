var async = require("async");
var InMemoryEventBus;

function InMemoryEventBusReceiver(options) {
  InMemoryEventBus = InMemoryEventBus || require("../in_memory");

  if (!options.logger)
    throw new Error("Missing logger");
  if (!options.queueName)
    throw new Error("Missing queue name");

  var self = this;

  self.eventHandlers = {};
  self.logger        = options.logger;
  self.queue         = InMemoryEventBus.registerQueue({
    name: options.queueName,
    logger: self.logger
  });
  self.queue.registerHandler(function (event, callback) {
    // todo: drop EventEmitter to handle callbacks and return errors when needed
    var eventHandlers = self.eventHandlers[event.name];
    if (!eventHandlers || eventHandlers.length == 0)
      return callback();

    var parallelHandling = async.queue(function (eventHandler, callback) {
      eventHandler(event, callback);
    }, Infinity);
    parallelHandling.drain = callback;
    parallelHandling.push(eventHandlers);
  });
}

InMemoryEventBusReceiver.prototype.onEvent = function (eventName, options, handler) {
  var self = this;

  if (!handler) {
    handler = options;
    options  = {};
  }

  if (!self.eventHandlers[eventName])
    self.eventHandlers[eventName] = [];
  var eventHandlers     = self.eventHandlers[eventName];
  var eventHandlerIndex = eventHandlers.length;

  if (options.once) {
    var oneTimerHandler = handler;
    handler = function (event, callback) {
      eventHandlers.splice(eventHandlerIndex, 1);
      oneTimerHandler(event, callback);
    }
  }

  if (options.forAggregateUid) {
    var scopedHandler = handler;
    handler = function (event, callback) {
      if (event.aggregateUid === options.forAggregateUid) {
        scopedHandler(event, callback);
      }
    };
  }

  eventHandlers.push(handler);
};

module.exports = InMemoryEventBusReceiver;