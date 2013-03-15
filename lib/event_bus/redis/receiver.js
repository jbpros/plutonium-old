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

RedisEventBusReceiver.prototype.start = function (callback) {
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

  self.queue.initialize(function (err) {
    if (err) return callback(err);
    self.queue.start(function (err) {
      callback();
    });
  });
};

module.exports = RedisEventBusReceiver;
