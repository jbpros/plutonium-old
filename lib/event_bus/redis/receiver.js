var inherit                = require("../../inherit");
var CommonEventBusReceiver = require("../common/receiver");
var RedisEventBusQueue     = require("./queue");

inherit(RedisEventBusReceiver, CommonEventBusReceiver);

function RedisEventBusReceiver(options) {
  CommonEventBusReceiver.call(this, options);
  this.scope     = options.scope;
  this.host      = options.host;
  this.port      = options.port;
  this.replaying = !!options.replaying;
}

RedisEventBusReceiver.prototype.initialize = function (callback) {
  var self   = this;

  self.queue = new RedisEventBusQueue({
    name: self.queueName,
    host: self.host,
    port: self.port,
    logger: self.logger,
    scope: self.scope,
    replaying : self.replaying
  });

  self.queue.registerHandler(function (event, callback) {
    self._handleEvent(event, callback);
  });

  self.queue.initialize(function(err){
    if (err) return callback(err);
    self._subscribeEvents(callback);
  });
};

RedisEventBusReceiver.prototype.start = function (callback) {
  this.queue.start(callback);
};

RedisEventBusReceiver.prototype.emptyQueue = function (callback) {
  this.queue.empty(callback);
};

RedisEventBusReceiver.prototype._subscribeEvents = function (callback) {
  var eventNames = Object.keys(this.eventHandlers);
  this.queue.subscribeEvents(eventNames, callback);
};

module.exports = RedisEventBusReceiver;