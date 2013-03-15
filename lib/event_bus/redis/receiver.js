var inherit                = require("../../inherit");
var CommonEventBusReceiver = require("../common/receiver");
var RedisEventBusQueue     = require("./queue");

inherit(RedisEventBusReceiver, CommonEventBusReceiver);

function RedisEventBusReceiver(options) {
  CommonEventBusReceiver.call(this, options);
  this.scope              = options.scope;
  this.redisPort = options.redisPort;
  this.redisHost = options.redisHost;
}

RedisEventBusReceiver.prototype.start = function (callback) {
  var self   = this;

  self.queue = new RedisEventBusQueue({
    name: self.queueName,
    logger: self.logger,
    scope: self.scope,
    redisPort: self.redisPort,
    redisHost: self.redisHost
  });

  self.queue.registerHandler(function (event, callback) {
    self._handleEvent(event, callback);
  });

  self.queue.initialize(function (err) {
    if (err) return callback(err);
    self.queue.start(function (err) {
      callback();
    });
  });
};

module.exports = RedisEventBusReceiver;
