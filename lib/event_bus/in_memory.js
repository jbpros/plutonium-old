var async    = require("async");
var Emitter  = require("./in_memory/emitter");
var Receiver = require("./in_memory/receiver");
var Queue    = require("./in_memory/queue");

var InMemoryEventBus = {
  queues: {},

  broadcastEvent: function broadcastEvent(event, callback) {
    if (Object.keys(InMemoryEventBus.queues).length == 0) {
      return callback(null);
    }

    var parallelQueueing = async.queue(function (queue, cb) {
      queue.pushEvent(event, cb);
    }, Infinity);
    parallelQueueing.drain = callback;
    for (var queueName in InMemoryEventBus.queues) {
      var queue = InMemoryEventBus.queues[queueName];
      parallelQueueing.push(queue);
    }
  },

  registerQueue: function registerQueue(options) {
    if (!options.name)
      throw new Error("Missing queue name");
    if (!options.logger)
      throw new Error("Missing logger");

    if (InMemoryEventBus.queues[options.name])
      throw new Error("A queue with the same name (" + options.name  +") already exists");
    var queue = new Queue({name: options.name, logger: options.logger});
    InMemoryEventBus.queues[options.name] = queue;
    return queue;
  }
};

InMemoryEventBus.Emitter = Emitter;
InMemoryEventBus.Receiver = Receiver;
InMemoryEventBus.Queue = Queue;

module.exports = InMemoryEventBus;