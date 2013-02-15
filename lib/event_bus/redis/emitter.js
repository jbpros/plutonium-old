var async                 = require("async");
var redis                 = require("redis");
var inherit               = require("../../inherit");
var CommonEventBusEmitter = require("../common/emitter");
var RedisEventBusQueue    = require("./queue");

var RedisEventBus;

var QUEUE_KEY_MATCHER = RedisEventBusQueue.QUEUE_KEY_PREFIX + "*";

inherit(RedisEventBusEmitter, CommonEventBusEmitter);

function RedisEventBusEmitter(options) {
  CommonEventBusEmitter.call(this, options);
  RedisEventBus = RedisEventBus || require("../redis");
  this.started     = false;
  this.starting    = false;
  this.queuedCalls = [];
};

RedisEventBusEmitter.prototype.emit = function (event, callback) {
  var self = this;

  self._start(function (err) {
    if (err) return callback(err);
    self.queueManager.smembers(RedisEventBusQueue.QUEUE_SET_KEY, function (err, queueNames) {
      if (err) return callback(err);

      if (queueNames.length == 0)
        return callback();

      var transaction = self.queueWriter.multi();
      queueNames.forEach(function (queueName) {
        var key = RedisEventBusQueue.IN_QUEUE_LIST_KEY_PREFIX + queueName;
        var value = RedisEventBusQueue.serializeEvent(event);
        self.logger.log("RedisEventBusEmitter", "pushing event \"" + event.name + "\" to key \"" + key + "\"");
        transaction.lpush(key, value);
      });
      transaction.exec(function (err, _) { callback(err); });
    });
  });
};

RedisEventBusEmitter.prototype.clearQueues = function (callback) {
  var self = this;

  self._start(function (err) {
    self.queueManager.keys(QUEUE_KEY_MATCHER, function (err, keys) {
      var transaction = self.queueManager.multi();
      keys.forEach(function (key) {
        self.logger.log("RedisEventBusEmitter", "(clear queues) delete key \"" + key + "\"");
        transaction.del(key);
      });
      transaction.exec(function (err) {
        self.logger.notice("RedisEventBusEmitter", "clearing all queues");
        setTimeout(function() {callback(err)}, 1000);
      });
    });
  });
};

RedisEventBusEmitter.prototype._start = function (callback) {
  var self = this;

  if (self.started)
    return callback();

  self.queuedCalls.unshift(callback);

  if (self.starting)
    return;

  self.starting = true;

  async.series([
    function (next) {
      self.queueManager = redis.createClient();
      self.queueManager.on("error", function (err) {
        self.logger.alert("RedisEventBusEmitter", "queue manager raised an error: " + err);
      });
      self.queueManager.on("ready", next);
    },
    function (next) {
      self.queueWriter = redis.createClient();
      self.queueWriter.on("error", function (err) {
        self.logger.alert("RedisEventBusEmitter", "queue writer raised an error: " + err);
      });
      self.queueWriter.on("ready", next);
    },
    function (next) {
      var queuedCall;
      while (queuedCall = self.queuedCalls.pop()) {
        queuedCall();
      }
      self.started  = true;
      self.starting = false;
      next();
    }
  ], function (err) {
    if (err)
      throw err; // TODO: improve error handling
  });
};

module.exports = RedisEventBusEmitter;
