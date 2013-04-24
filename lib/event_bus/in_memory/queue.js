var inherit             = require("../../inherit");
var CommonEventBusQueue = require("../common/queue");
var defer               = require("../../defer");

inherit(InMemoryEventBusQueue, CommonEventBusQueue);

function InMemoryEventBusQueue(options) {
  CommonEventBusQueue.call(this, options);
  this.events     = [];
  this.processing = false;
  this.retries    = 0;
}

InMemoryEventBusQueue.prototype.pushEvent = function (event, callback) {
  var self = this;

  if (self.stopped) // simply ignore the event
    return callback();

  self.events.push(event);
  defer(function () {
    self._process();
  });
  callback();
};

InMemoryEventBusQueue.prototype._process = function () {
  var self = this;

  if (self.stopped || self.processing)
    return;

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
      defer(function () {
        self._process();
      });
    });
  }
};

module.exports = InMemoryEventBusQueue;
