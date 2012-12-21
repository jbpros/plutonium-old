function InMemoryEventBusQueue(options) {
  options = options || {};
  if (!options.name)
    throw new Error("Missing queue name");
  if (!options.logger)
    throw new Error("Missing logger");

  this.name       = options.name;
  this.logger     = options.logger;
  this.events     = [];
  this.processing = false;
  this.retries    = 0;
}

InMemoryEventBusQueue.prototype.pushEvent = function (event, callback) {
  var self = this;
  self.events.push(event);
  process.nextTick(function () {
    self._process();
  });
  callback();
};

InMemoryEventBusQueue.prototype.registerHandler = function (handler) {
  if (this.handler)
    throw new Error("A handler has been registered already.");
  this.handler = handler;
};

InMemoryEventBusQueue.prototype._process = function () {
  var self = this;

  if (self.processing)
    return; // prevent another event to be processed while an asynchronous
            // operation is taking place on a previous event.

  var event = self.events[0];
  if (event) {
    self.processing = true;

    self.handler(event, function (err) {
      // TODO: handle errors
      self.logger.debug("InMemoryEventBusQueue#_process", "TODO: handle errors, keep event in queue?");
      if (err) {
        self.logger.warning("InMemoryEventBusQueue#_process", "an error occurred (" + err + "), " + self.retries + " retries.");
        self.retries++;
      } else {
        self.events.shift();
        self.retries = 0;
      }
      self.processing = false;
      process.nextTick(function () {
        self._process();
      });
    });
  }
};

module.exports = InMemoryEventBusQueue;