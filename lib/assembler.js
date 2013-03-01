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
    for (var key in self.configuration.app) {
      appOptions[key] = self.configuration.app[key];
    }
  }

  var createApp = function createApp() {
    self.app = new self.App(appOptions);
    callback();
  };

  if (self.configuration.domain) {
    self._assembleDomainInfrastructure(function(err){
      if (err)
        return callback(err);
      appOptions.domainRepository = self.domainRepository;
      appOptions.commandBus       = self.commandBus;
      createApp();
    });
  } else {
    createApp();
  }
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
      logger: self.logger,
      scope: self.configuration.scope
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

  if (self.reportStores.length == 0)
    return callback();

  reportStoreInitQueue = async.queue(function (reportStore, taskCallback) {
    if (reportStore.initialize){
      reportStore.initialize(taskCallback);
    } else {
      taskCallback();
    }
  }, Infinity);

  reportStoreInitQueue.drain = callback;

  reportStoreInitQueue.push(self.reportStores);
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
      self.destroyEventBusEmitter(next);
    },
    function (next) {
      self.unloadEventBusReceivers(next);
    },
    function (next) {
      self.destroyReporters(next);
    },
    function (next) {
      self.destroyReportStores(next);
    },
    function (next) {
      self.destroyEventStore(next);
    }
  ], function (err) {
    callback(err);
  });
};

Assembler.prototype.destroyEventBusEmitter = function (callback) {
  var self = this;
  if (self.eventBusEmitter.destroy)
    self.eventBusEmitter.destroy(callback);
  else
    callback();
}

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

Assembler.prototype.destroyReportStores = function (callback) {
  var self = this;

  if (self.reportStores.length == 0)
    return callback();

  var queue = async.queue(function (reportStore, callback) {
    if (reportStore.destroy) {
      reportStore.destroy(function (err) {
        if (err)
          self.logger.error("Assembler#destroyReportStores", "destroying report store failed: " + err);
        callback(err);
      });
    } else {
      callback();
    }
  }, Infinity);

  queue.drain = function (err) {
    if (err)
      self.logger.error("Assembler#destroyReportStores", "error: " + err);
    self.reportStores = [];
    callback(err);
  };
  queue.push(self.reportStores);
};

Assembler.prototype.destroyEventStore = function (callback) {
  if (this.eventStore.destroy) {
    this.eventStore.destroy(callback);
  } else {
    callback();
  }
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

Assembler.prototype._assembleDomainInfrastructure = function (callback) {
  var self = this;

  var EventStore       = self.configuration.domain.eventStore.ctor;
  var eventStore       = new EventStore({
    uri: self.configuration.domain.eventStore.uri,
    logger: self.logger
  });

  var initializeDomainInfrastructure = function initializeDomainInfrastructure(callback) {
    var EventBusEmitter  = self.configuration.domain.eventBusEmitter.ctor;
    var eventBusEmitter  = new EventBusEmitter({
      logger: self.logger,
      scope: self.configuration.scope
    });
    var domainRepository = new DomainRepository({
      store: eventStore,
      emitter: eventBusEmitter,
      logger: self.logger
    });
    var commandBus = new CommandBus({
      domainRepository: domainRepository,
      port: self.configuration.domain.commandBus && self.configuration.domain.commandBus.port,
      logger: self.logger
    });

    // These initializations take place on the constructor, not instances.
    // This is potentially buggy as a second in-process assembling could lead to
    // overwriting the previous command bus and domain repository instances.
    // TODO: Fix that.
    Entity.initialize({
      commandBus:       commandBus,
      domainRepository: domainRepository,
      logger:           self.logger
    });

    Service.initialize({
      commandBus:       commandBus,
      domainRepository: domainRepository,
      logger:           self.logger
    });

    self.eventBusEmitter  = eventBusEmitter;
    self.domainRepository = domainRepository;
    self.commandBus       = commandBus;
    self.eventStore       = eventStore;

    callback();
  };


  if (eventStore.initialize) {
    eventStore.initialize(function(err){
      if (err)
        return callback(err);
      initializeDomainInfrastructure(callback);
    });
  } else {
    initializeDomainInfrastructure(callback);
  }
};

module.exports = Assembler;
