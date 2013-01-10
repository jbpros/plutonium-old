var inherit                = require("../../inherit");
var CommonEventBusReceiver = require("../common/receiver");
var RedisEventBusQueue     = require("./queue");

inherit(RedisEventBusReceiver, CommonEventBusReceiver);

function RedisEventBusReceiver(options) {
  CommonEventBusReceiver.call(this, options);
}

RedisEventBusReceiver.prototype.start = function (callback) {
  var self   = this;

  self.queue = new RedisEventBusQueue({
    name: self.queueName,
    logger: self.logger
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
