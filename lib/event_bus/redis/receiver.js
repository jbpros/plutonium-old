var inherit                = require("../../inherit");
var CommonEventBusReceiver = require("../common/receiver");
var RedisEventBusQueue     = require("./queue");

inherit(RedisEventBusReceiver, CommonEventBusReceiver);

function RedisEventBusReceiver(options) {
  CommonEventBusReceiver.call(this, options);
  this.scope = options.scope;
  this.host  = options.host;
  this.port  = options.port;
}

RedisEventBusReceiver.prototype.initialize = function (callback) {
  var self   = this;

  self.queue = new RedisEventBusQueue({
    name: self.queueName,
    host: self.host,
    port: self.port,
    logger: self.logger,
    scope: self.scope
  });

  self.queue.registerHandler(function (event, callback) {
    self._handleEvent(event, callback);
  });

  self.queue.initialize(callback);
};

RedisEventBusReceiver.prototype.start = function (callback) {
  this.queue.start(callback);
};

RedisEventBusReceiver.prototype.emptyQueue = function (callback) {
  this.queue.empty(callback);
};

module.exports = RedisEventBusReceiver;
