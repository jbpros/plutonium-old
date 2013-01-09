var inherit             = require("../../inherit");
var CommonEventBusQueue = require("../common/queue");

inherit(InMemoryEventBusQueue, CommonEventBusQueue);

function InMemoryEventBusQueue(options) {
  this._super(options);
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

InMemoryEventBusQueue.prototype._process = function () {
  var self = this;

  if (self.processing)
    return; // prevent another event to be processed while an asynchronous
            // operation is taking place on a previous event.

  var event = self.events[0];
  if (event) {
    self.processing = true;

    self.handler(event, function (err) {
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
