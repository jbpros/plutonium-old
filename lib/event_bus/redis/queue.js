var async               = require("async");
var redis               = require("redis");
var inherit             = require("../../inherit");
var CommonEventBusQueue = require("../common/queue");
var Event               = require("../../event");
var RedisEventBus;
var BSON                = require("bson").pure().BSON;

inherit(RedisEventBusQueue, CommonEventBusQueue);

var QUEUE_KEY_PREFIX          = "event-bus:queues:";
var QUEUE_SET_KEY             = QUEUE_KEY_PREFIX;
var IN_QUEUE_LIST_KEY_PREFIX  = QUEUE_KEY_PREFIX + "in:";
var OUT_QUEUE_LIST_KEY_PREFIX = QUEUE_KEY_PREFIX + "out:";
var LAST_KEY_INDEX            = -1;

function RedisEventBusQueue(options) {
  CommonEventBusQueue.call(this, options);
  RedisEventBus = RedisEventBus || require("../redis");
  this.retries  = 0;
}

RedisEventBusQueue.initialize = function (options) {
  if (!this.isInitialized) {

    if (options.scope) {
      QUEUE_KEY_PREFIX          = QUEUE_KEY_PREFIX + options.scope + ":";
      QUEUE_SET_KEY             = QUEUE_KEY_PREFIX;
      IN_QUEUE_LIST_KEY_PREFIX  = QUEUE_KEY_PREFIX + "in:";
      OUT_QUEUE_LIST_KEY_PREFIX = QUEUE_KEY_PREFIX + "out:";
    }

    RedisEventBusQueue.QUEUE_KEY_PREFIX          = QUEUE_KEY_PREFIX;
    RedisEventBusQueue.QUEUE_SET_KEY             = QUEUE_SET_KEY;
    RedisEventBusQueue.IN_QUEUE_LIST_KEY_PREFIX  = IN_QUEUE_LIST_KEY_PREFIX;
    RedisEventBusQueue.OUT_QUEUE_LIST_KEY_PREFIX = OUT_QUEUE_LIST_KEY_PREFIX;
    RedisEventBusQueue.LAST_KEY_INDEX            = LAST_KEY_INDEX;

    this.isInitialized = true;
  }
}

RedisEventBusQueue.prototype.initialize = function (callback) {
  var self = this;

  async.series([
    function (next) {
      self.queueManager = redis.createClient(null, null, { return_buffers: true });;
      self.queueManager.on("error", function (err) {
        self.logger.alert("RedisEventBusReceiver", "queue manager raised an error: " + err);
      });
      self.queueManager.on("ready", next);
    },
    function (next) {
      self.queueManager.sadd(QUEUE_SET_KEY, self.name, next);
    },
    function (next) {
      self.queueReader = redis.createClient(null, null, { return_buffers: true });;
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
  var self = this;
  var logger = self.logger;
  var inKey  = IN_QUEUE_LIST_KEY_PREFIX + self.name;
  var outKey = OUT_QUEUE_LIST_KEY_PREFIX + self.name;

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
        var event = self.constructor.deserializeEvent(serializedEvent);
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

RedisEventBusQueue.serializeEvent = function (event) {
  var res = BSON.serialize({
    name: event.name,
    data: event.data,
    uid: event.uid,
    aggregateUid: event.aggregateUid
  });
  return res
};

RedisEventBusQueue.deserializeEvent = function (string) {
  obj = cleanBSON(BSON.deserialize(string));
  var event = new Event(obj.name, obj.data, obj.uid, obj.aggregateUid);
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