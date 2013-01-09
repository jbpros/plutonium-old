function CommonEventBusQueue(options) {
  options = options || {};
  if (!options.name)
    throw new Error("Missing queue name");
  if (!options.logger)
    throw new Error("Missing logger");

  this.name    = options.name;
  this.logger  = options.logger;
  this.stopped = false;
  this.handler = null;
};

CommonEventBusQueue.prototype.registerHandler = function (handler) {
  if (this.handler)
    throw new Error("A handler has been registered already.");
  this.handler = handler;
};

CommonEventBusQueue.prototype.stop = function (callback) {
  this.stopped = true;
  callback();
};

module.exports = CommonEventBusQueue;
