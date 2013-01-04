var async            = require("async");
var DomainRepository = require("./domain_repository");
var CommandBus       = require("./command_bus");
var Configuration    = require("./configuration");
var Entity           = require("./entity");
var Service          = require("./service");
var logger           = require("./logger");

function Assembler(App, configurationPath) {
  if (!App)
    throw new Error("Missing app constructor");
  if (!configurationPath)
    throw new Error("Missing configuration file path");

  this.App       = App;
  this.configurationPath = configurationPath;
  this.logger    = logger;
  this.reporters = [];
}

// Assemble an app, load the reporters.
Assembler.prototype.loadCompleteApp = function () {
  this.assembleApp();
  this.loadReporters();
};

// Assemble an app only, no reporters
Assembler.prototype.assembleApp = function () {
  if (this.app)
    throw new Error("An app was assembled already.");

  this._configure();

  var appOptions = {
    logger: this.logger,
    assembler: this
  };

  if (this.configuration.app) {
    for (var key in this.configuration.app) {
      appOptions[key] = this.configuration.app[key];
    }
  }

  if (this.configuration.domain) {
    this.assembleDomainInfrastructure();
    appOptions.domainRepository = this.domainRepository;
    appOptions.commandBus       = this.commandBus;
  }

  this.app = new this.App(appOptions);
};

Assembler.prototype.assembleDomainInfrastructure = function () {
  var EventStore       = this.configuration.domain.eventStore.ctor;
  var eventStore       = new EventStore({
    uri: this.configuration.domain.eventStore.uri,
    logger: this.logger
  });
  var EventBusEmitter  = this.configuration.domain.eventBusEmitter.ctor;
  var eventBusEmitter  = new EventBusEmitter(); // todo: pass config
  var domainRepository = new DomainRepository({
    store: eventStore,
    emitter: eventBusEmitter,
    logger: this.logger
  });
  var commandBus = new CommandBus({
    domainRepository: domainRepository,
    logger: this.logger
  });

  // These initializations take place on the constructor, not instances.
  // This is potentially buggy as a second in-process assembling could lead to
  // overwriting the previous command bus and domain repository instances.
  // TODO: Fix that.
  Entity.initialize({
    commandBus:       commandBus,
    domainRepository: domainRepository,
    logger:           this.logger
  });

  Service.initialize({
    commandBus:       commandBus,
    domainRepository: domainRepository,
    logger:           this.logger
  });

  this.eventBusEmitter  = eventBusEmitter;
  this.domainRepository = domainRepository;
  this.commandBus       = commandBus;
};

// Load the reporters
Assembler.prototype.loadReporters = function () {
  var self = this;

  if (!self.app)
    throw new Error("An app must be assembled first.");

  var reportersSettings = self.configuration.reporters;
  var logger            = self.logger;
  var reporters         = [];

  reportersSettings.forEach(function (reporterSettings) {
    var EventBusReceiver  = reporterSettings.eventBusReceiver.ctor;
    var eventBusReceiver = new EventBusReceiver({
      queueName: reporterSettings.eventBusReceiver.queue,
      logger: self.logger,
      configuration: reporterSettings.configuration
    });
    var reporterOptions = {
      eventBusReceiver: eventBusReceiver,
      logger: logger,
      configuration: reporterSettings.configuration
    };
    var reporter = new reporterSettings.ctor(reporterOptions);
    reporters.push(reporter);
  });
  this.reporters = reporters;
};

// Setup the domain event store (be careful, you'll lose all domain events!)
Assembler.prototype.setupEventStore = function (callback) {
  if (!this.app)
    throw new Error("An app must be assembled first.");

  this.app.domainRepository.store.setup(callback);
};

// Setup the reporters (will destroy all reports)
Assembler.prototype.setupReporters = function (callback) {
  if (!this.reporters)
    throw new Error("Reporters must be loaded first.");
  if (this.reporters.length == 0)
    callback();

  var queue = async.queue(function (reporter, callback) {
    reporter.setup(callback);
  }, Infinity);

  queue.drain = callback;

  this.reporters.forEach(function (reporter) {
    queue.push(reporter);
  });
};

Assembler.prototype._configure = function (defaultEnv) {
  this.configuration = new Configuration(this.configurationPath, process.env.NODE_ENV || defaultEnv || "development");
};

module.exports = Assembler;
