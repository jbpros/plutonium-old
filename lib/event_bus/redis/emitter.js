var async                 = require("async");
var redis                 = require("redis");
var inherit               = require("../../inherit");
var CommonEventBusEmitter = require("../common/emitter");
var RedisEventBusQueue    = require("./queue");

var RedisEventBus;

inherit(RedisEventBusEmitter, CommonEventBusEmitter);

function RedisEventBusEmitter(options) {
  CommonEventBusEmitter.call(this, options);
  RedisEventBus = RedisEventBus || require("../redis");
  this.started     = false;
  this.starting    = false;
  options.name     = "emitter";
  this.queue       = new RedisEventBusQueue(options);
  this.host        = options.host;
  this.port        = options.port;
  this.queuedCalls = [];
};

RedisEventBusEmitter.prototype.emit = function (event, callback) {
  var self = this;

  self._start(function (err) {
    if (err) return callback(err);
    self.queueManager.smembers(self.queue.getQueueSetKey(), function (err, queueNames) {
      if (err) return callback(err);

      if (queueNames.length == 0)
        return callback();

      var transaction = self.queueWriter.multi();
      queueNames.forEach(function (queueName) {
        var key = self.queue.getInQueueListKeyPrefix() + queueName;
        var value = self.queue.serializeEvent(event);
        self.logger.log("RedisEventBusEmitter", "pushing event \"" + event.name + "\" to key \"" + key + "\"");
        transaction.lpush(key, value);
      });
      transaction.exec(function (err, _) { callback(err); });
    });
  });
};

RedisEventBusEmitter.prototype.clearQueues = function (callback) {
  var queueKeyMatcher = this.queue.getQueueKeyPrefix() + "*";

  this._removeQueues(queueKeyMatcher, callback);
};

RedisEventBusEmitter.prototype.emptyQueues = function (callback) {
  var inQueueKeyPrefix  = this.queue.getInQueueListKeyPrefix();
  var outQueueKeyPrefix = this.queue.getOutQueueListKeyPrefix();
  var queueKeyMatcher   = this.queue.getQueueKeyPrefix() + "[" + inQueueKeyPrefix + "|" + outQueueKeyPrefix + "]";

  this._removeQueues(queueKeyMatcher, callback);
};

RedisEventBusEmitter.prototype.unsubscribeAllReceivers = function (callback) {
  var self = this;
  self._start(function (err) {
    if (err) return callback(err);
    self.queueManager.del(self.queue.getQueueSetKey(), function (err, _) {
      callback(err);
    });
  });
};

RedisEventBusEmitter.prototype._removeQueues = function (queueKeyMatcher, callback) {
  var self = this;
  self._start(function (err) {
    self.queueManager.keys(queueKeyMatcher, function (err, keys) {
      var transaction = self.queueManager.multi();
      keys.forEach(function (key) {
        self.logger.log("RedisEventBusEmitter", "(clear queues) delete key \"" + key + "\"");
        transaction.del(key);
      });
      transaction.exec(function (err) {
        self.logger.notice("RedisEventBusEmitter", "clearing all queues");
        callback(err);
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
      self.queueManager = redis.createClient(self.port, self.host);
      self.queueManager.on("error", function (err) {
        self.logger.alert("RedisEventBusEmitter", "queue manager raised an error: " + err);
      });
      self.queueManager.on("ready", next);
    },
    function (next) {
      self.queueWriter = redis.createClient(self.port, self.host);
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

RedisEventBusEmitter.prototype.destroy = function (callback) {
  var self = this;

  if (self.started) {
    self.queueManager.end();
    self.queueWriter.end();
  }
  callback();
};

module.exports = RedisEventBusEmitter;