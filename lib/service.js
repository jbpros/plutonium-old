function Service() {
  this.constructor._checkDependencies();
  this.logger           = Service.logger;
  this.commandBus       = Service.commandBus;
  this.domainRepository = Service.domainRepository;
};

Service.initialize = function(options) {
  if (!options.logger)           throw new Error("Missing logger");
  if (!options.commandBus)       throw new Error("Missing command bus");
  if (!options.domainRepository) throw new Error("Missing domain repository");

  Service.logger           = options.logger;
  Service.commandBus       = options.commandBus;
  Service.domainRepository = options.domainRepository;
};

Service._checkDependencies = function() {
  if (!Service.logger)     throw new Error("Logger instance not set on constructor");
  if (!Service.commandBus) throw new Error("Command bus instance not set on constructor");
};

module.exports = Service;