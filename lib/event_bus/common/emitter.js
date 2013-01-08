function CommonEventBusEmitter(options) {
  options = options || {};
  if (!options.logger)
    throw new Error("Missing logger");

  this.logger = options.logger;
}

module.exports = CommonEventBusEmitter;
