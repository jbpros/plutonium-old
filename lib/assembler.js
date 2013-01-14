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

  this.App               = App;
  this.configurationPath = configurationPath;
  this.logger            = logger;
  this.reporters         = [];
  this.reports           = [];
  this.reportStores      = [];
  this.eventBusReceivers = [];
}

// Assemble an app, load the reporters.
Assembler.prototype.loadCompleteApp = function (options, callback) {
  var self = this;

  if (!callback) {
    callback = options;
    options  = {
      clearQueues: false,
      setupEventStore: false,
      setupReportStores: false
    };
  }

  async.series([
    function (next) {
      self.assembleApp(next);
    },
    function (next) {
      if (options.clearQueues)
        self.eventBusEmitter.clearQueues(next);
      else
        next();
    },
    function (next) {
      self.initializeReports(next);
    },
    function (next) {
      self.loadReporters(next);
    },
    function (next) {
      if (options.setupEventStore)
        self.setupEventStore(next);
      else
        next();
    },
    function (next) {
      if (options.setupReportStores)
        self.setupReportStores(next);
      else
        next();
    }
  ], function (err) {
    callback(err);
  });
};

// Assemble an app only, no reporters
Assembler.prototype.assembleApp = function (callback) {
  var self = this;

  if (self.app)
    throw new Error("An app was assembled already.");

  self.loadConfiguration();

  var appOptions = {
    logger: self.logger,
    assembler: self
  };

  if (self.configuration.app) {
    for (var key in this.configuration.app) {
      appOptions[key] = this.configuration.app[key];
    }
  }

  if (this.configuration.domain) {
    this._assembleDomainInfrastructure();
    appOptions.domainRepository = this.domainRepository;
    appOptions.commandBus       = this.commandBus;
  }

  this.app = new this.App(appOptions);
  callback();
};

Assembler.prototype.loadConfiguration = function (defaultEnv) {
  this.configuration = new Configuration(this.configurationPath, process.env.NODE_ENV || defaultEnv || "development");
};

// Load the reporters
Assembler.prototype.loadReporters = function (callback) {
  var self = this;

  if (!self.app)
    return callback(new Error("An app must be assembled first."));

  var reportersSettings = self.configuration.reporters || [];
  var logger            = self.logger;

  if (reportersSettings.length == 0)
    return callback();

  var queue = async.queue(function (reporterSettings, callback) {
    var EventBusReceiver  = reporterSettings.eventBusReceiver.ctor;
    var eventBusReceiver = new EventBusReceiver({
      queueName: reporterSettings.eventBusReceiver.queue,
      logger: self.logger
    });
    var reporterOptions = {
      eventBusReceiver: eventBusReceiver,
      logger: logger
    };
    var reporter = new reporterSettings.ctor(reporterOptions);
    self.reporters.push(reporter);
    self.eventBusReceivers.push(eventBusReceiver);
    eventBusReceiver.start(callback);
  }, Infinity);

  queue.drain = callback;

  queue.push(reportersSettings);
};

// Initialize the reports
Assembler.prototype.initializeReports = function (callback) {
  var self = this;

  if (!self.app)
    return callback(new Error("An app must be assembled first."));

  var reportsSettings = self.configuration.reports || [];

  reportsSettings.forEach(function (reportSettings) {
    var Report      = reportSettings.ctor;
    var ReportStore = reportSettings.store.ctor;
    var reportStore = new ReportStore({
      uri: reportSettings.store.uri,
      logger: self.logger
    });
    Report.initialize({store: reportStore, logger: self.logger});
    self.reports.push(Report);
    self.reportStores.push(reportStore);
  });

  callback();
};

Assembler.prototype.setupEventStore = function (callback) {
  if (!this.app)
    throw new Error("An app must be assembled first.");

  this.app.domainRepository.store.setup(callback);
};

// Setup the reports (will destroy all reports)
Assembler.prototype.setupReportStores = function (callback) {
  if (!this.reportStores)
    throw new Error("Reports must be loaded first.");
  if (this.reportStores.length == 0)
    callback();

  var queue = async.queue(function (reportStore, callback) {
    reportStore.setup(callback);
  }, Infinity);

  queue.drain = callback;

  this.reportStores.forEach(function (reportStore) {
    queue.push(reportStore);
  });
};

Assembler.prototype.tearDownApp = function (callback) {
  var self = this;

  // TODO: review this, anything else to unload/destroy?

  async.series([
    function (next) {
      self.destroyReporters(next);
    },
    function (next) {
      self.unloadEventBusReceivers(next);
    }
  ], function (err) {
    callback(err);
  });
};

Assembler.prototype.destroyReporters = function (callback) {
  var self = this;

  if (self.reporters.length == 0)
    return callback();

  var queue = async.queue(function (reporter, callback) {
    reporter.destroy(function (err) {
      if (err)
        self.logger.error("Assembler#unloadReporters", "destroying reporter failed: " + err);
      callback(err);
    });
  }, Infinity);

  queue.drain = function (err) {
    if (err)
      self.logger.error("Assembler#unloadReporters", "error: " + err);
    self.reporters = [];
    callback(err);
  };
  queue.push(self.reporters);
};

Assembler.prototype.unloadEventBusReceivers = function (callback) {
  var self = this;
  if (self.eventBusReceivers.length == 0)
    return callback();

  var queue = async.queue(function (eventBusReceiver, callback) {
    eventBusReceiver.stop(function (err) {
      if (err)
        self.logger.error("Assembler#unloadEventBusReceivers", "stopping event bus receiver failed: " + err);
      callback(err);
    });
  }, Infinity);

  queue.drain = function (err) {
    if (err)
      self.logger.error("Assembler#unloadEventBusReceivers", "error: " + err);
    self.eventBusReceivers = [];
    callback(err);
  };
  queue.push(self.eventBusReceivers);
};

Assembler.prototype._assembleDomainInfrastructure = function () {
  var EventStore       = this.configuration.domain.eventStore.ctor;
  var eventStore       = new EventStore({
    uri: this.configuration.domain.eventStore.uri,
    logger: this.logger
  });
  var EventBusEmitter  = this.configuration.domain.eventBusEmitter.ctor;
  var eventBusEmitter  = new EventBusEmitter({
    logger: this.logger
  });
  var domainRepository = new DomainRepository({
    store: eventStore,
    emitter: eventBusEmitter,
    logger: this.logger
  });
  var commandBus = new CommandBus({
    domainRepository: domainRepository,
    port: this.configuration.domain.commandBus && this.configuration.domain.commandBus.port,
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

module.exports = Assembler;
