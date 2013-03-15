var async               = require("async");
var redis               = require("redis");
var inherit             = require("../../inherit");
var CommonEventBusQueue = require("../common/queue");
var Event               = require("../../event");
var BSON                = require("bson").pure().BSON;
var RedisEventBus;

inherit(RedisEventBusQueue, CommonEventBusQueue);

var QUEUE_KEY_SEPARATOR  = ":";
var QUEUE_KEY_PREFIX     = "event-bus" + QUEUE_KEY_SEPARATOR + "queues" + QUEUE_KEY_SEPARATOR;
var IN_QUEUE_KEY_PREFIX  = "in" + QUEUE_KEY_SEPARATOR;
var OUT_QUEUE_KEY_PREFIX = "out" + QUEUE_KEY_SEPARATOR;
var LAST_KEY_INDEX       = -1;

function RedisEventBusQueue(options) {
  CommonEventBusQueue.call(this, options);
  RedisEventBus = RedisEventBus || require("../redis");
  this.retries  = 0;
  this.scope    = options.scope;
  this.host     = options.host;
  this.port     = options.port;
}

RedisEventBusQueue.prototype.getQueueKeyPrefix = function () {
  if (this.scope)
    return this.scope + QUEUE_KEY_SEPARATOR + QUEUE_KEY_PREFIX;
  else
    return QUEUE_KEY_PREFIX + QUEUE_KEY_SEPARATOR;
};

RedisEventBusQueue.prototype.getQueueSetKey = RedisEventBusQueue.prototype.getQueueKeyPrefix;

RedisEventBusQueue.prototype.getInQueueListKeyPrefix = function () {
  return this.getQueueKeyPrefix() + IN_QUEUE_KEY_PREFIX;
};

RedisEventBusQueue.prototype.getOutQueueListKeyPrefix = function () {
  return this.getQueueKeyPrefix() + OUT_QUEUE_KEY_PREFIX;
};

RedisEventBusQueue.prototype.initialize = function (callback) {
  var self = this;

  async.series([
    function (next) {
      self.queueManager = redis.createClient(self.port, self.host, { return_buffers: true });;
      self.queueManager.on("error", function (err) {
        self.logger.alert("RedisEventBusReceiver", "queue manager raised an error: " + err);
      });
      self.queueManager.on("ready", next);
    },
    function (next) {
      self.queueManager.sadd(self.getQueueSetKey(), self.name, next);
    },
    function (next) {
      self.queueReader = redis.createClient(self.port, self.host, { return_buffers: true });;
      self.queueReader.on("error", function (err) {
        self.logger.alert("RedisEventBusReceiver", "queue reader raised an error: " + err);
      });
      self.queueReader.on("ready", next);
    }
  ], callback);
};

RedisEventBusQueue.prototype.stop = function (callback) {
  var self = this;

  self.stopped = true;
  self.queueManager.end();
  self.queueReader.end();
  callback();
};

RedisEventBusQueue.prototype.start = function (callback) {
  var self   = this;
  var logger = self.logger;
  var inKey  = self.getInQueueListKeyPrefix() + self.name;
  var outKey = self.getOutQueueListKeyPrefix() + self.name;

  function readEvent() {
    if (self.stopped) return;

    self.logger.debug("RedisEventBusQueue", "reading queue " + inKey + " for next event...");
    self.queueReader.lrange(outKey, LAST_KEY_INDEX, LAST_KEY_INDEX, function (err, results) {
      if (self.stopped) return;

      if (err) {
        self.logger.error("RedisEventBusQueue", "reading event failed #{err}");
        process.nextTick(readEvent);
        return
      }

      var serializedEvent = results[0];

      if (serializedEvent) {
        var event = self.deserializeEvent(serializedEvent);
        self.logger.log("RedisEventBusQueue", "got event \"" + event.name + "\" (" + event.uid + ") from aggregate " + event.aggregateUid + " (queue: " + self.name + ")");

        self.handler(event, function (err) {
          if (err) {
            self.logger.warning("RedisEventBusQueue", "(handle event) an error occurred (" + err + "), " + self.retries + " retries.");
            self.retries++;
            process.nextTick(readEvent);
          } else {
            self.queueReader.rpop(outKey, function (err) {
              if (err)
                self.logger.error("RedisEventBusQueue(readEvent)", "(remove processed event) an error occurred: " + err + " - this can lead to duplicates!");
              process.nextTick(readEvent);
            });
          }

        });
      } else {
        self.logger.debug("RedisEventBusQueue", "(pulling event) from queue " + inKey);
        self.queueReader.brpoplpush(inKey, outKey, 0, function (err, results) {
          if (err)
            self.logger.error("RedisEventBusQueue", "pulling event failed #{err}");
          process.nextTick(readEvent);
        });
      }
    });
  }

  process.nextTick(readEvent);
  callback();
};

RedisEventBusQueue.prototype.serializeEvent = function (event) {
  var res = BSON.serialize({
    name: event.name,
    data: event.data,
    uid: event.uid,
    aggregateUid: event.aggregateUid,
    timestamp: event.timestamp
  });
  return res
};

RedisEventBusQueue.prototype.deserializeEvent = function (string) {
  obj = cleanBSON(BSON.deserialize(string));
  var event = new Event({
    name: obj.name,
    data: obj.data,
    uid: obj.uid,
    aggregateUid: obj.aggregateUid,
    timestamp: obj.timestamp,
    version: obj.version
  });
  return event;
};

var cleanBSON = function cleanBSON(object) {
  for (key in object) {
    var value = object[key];

    if (value && value._bsontype) {
      object[key] = value.buffer
    } else if (typeof value == "object") {
      object[key] = cleanBSON(value);
    }
  }
  return object;
};

module.exports = RedisEventBusQueue;