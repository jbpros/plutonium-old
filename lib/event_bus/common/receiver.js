var async = require("async");

function CommonEventBusReceiver(options) {
  options = options || {};

  if (!options.queueName)
    throw new Error("Missing queue name");
  if (!options.logger)
    throw new Error("Missing logger");

  this.queueName     = options.queueName;
  this.logger        = options.logger;
  this.eventHandlers = {};
}

CommonEventBusReceiver.prototype.onEvent = function (eventName, options, handler) {
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

CommonEventBusReceiver.prototype._handleEvent = function (event, callback) {
  var eventHandlers = this.eventHandlers[event.name];
  if (!eventHandlers || eventHandlers.length == 0)
    return callback();

  var parallelHandling = async.queue(function (eventHandler, callback) {
    eventHandler(event, callback);
  }, Infinity);
  parallelHandling.drain = callback;
  parallelHandling.push(eventHandlers);
};

CommonEventBusReceiver.prototype.stop = function (callback) {
  this.queue.stop(callback);
};

module.exports = CommonEventBusReceiver;
