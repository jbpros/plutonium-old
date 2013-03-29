var DomainObject = require("./domain_object");
var Snapshot     = require("./snapshot");

var SNAPSHOT_MAX_AGE = 99;

// todo: use one instance all the time?
function AggregateInstantiator(options) {
  if (!options.store) throw new Error("Missing store");
  if (!options.AggregateRoot) throw new Error("Missing aggregate root constructor");
  if (!options.logger) throw new Error("Missing logger");
  this.store  = options.store;
  this.AggregateRoot = options.AggregateRoot;
  this.logger = options.logger;
}

AggregateInstantiator.prototype.findByUid = function (uid, callback) {
  var self = this;
  self.store.loadSnapshotForAggregateUid(uid, function (err, snapshot) {
    if (err) return callback(err);

    if (snapshot) {
      self._instantiateFromSnapshot(snapshot, callback);
    } else {
      self._instantiateThroughEventsFromUid(uid, callback);
    }
  });
};

AggregateInstantiator.prototype._instantiateFromSnapshot = function (snapshot, callback) {
  var self = this;

  var aggregate = DomainObject.deserialize(snapshot.contents);
  var uid       = snapshot.aggregateUid;
  var version   = snapshot.version;
  aggregate._initializeAtVersion(version);

  self.logger.log("AggregateInstantiator", "looking for events of aggregate \""+ uid + "\" after version " + version);
  self.store.findAllEventsByAggregateUidAfterVersion(uid, version, function (err, events) {
    self.logger.log("AggregateInstantiator", "found " + events.length + " event(s) of aggregate \""+ uid + "\" after version " + version);
    if (err) return callback(err);
    aggregate.applyEvents(events, function (err) {
      if (err) return callback(err);
      var snapshotAge = aggregate.$version - version;
      if (snapshotAge > SNAPSHOT_MAX_AGE) {
        self.logger.log("AggregateInstantiator", "updating outdated snapshot (" + snapshotAge + " versions) of aggregate \""+ uid + "\"");
        self._snapAggregateRoot(aggregate);
      }
      self.logger.log("AggregateInstantiator", "instantiated aggregate \""+ uid + "\" from snapshot at version " + version);
      callback(null, aggregate);
    });
  });
};

AggregateInstantiator.prototype._instantiateThroughEventsFromUid = function (uid, callback) {
  var self = this;

  self.store.findAllEventsByAggregateUid(uid, function (err, events) {
    if (err) {
      callback(err);
    } else if (events.length == 0) {
      callback(null, null);
    } else {
      self.AggregateRoot.buildFromEvents(events, function (err, aggregateRoot) {
        if (err) return callback(err);
        if (aggregateRoot.$version > SNAPSHOT_MAX_AGE)
          self._snapAggregateRoot(aggregateRoot);
        self.logger.log("AggregateInstantiator", "instantiated aggregate \""+ uid + "\" at version " + aggregateRoot.$version + " from " + events.length + " events");
        callback(null, aggregateRoot);
      });
    }
  });
};

AggregateInstantiator.prototype._snapAggregateRoot = function (aggregateRoot) {
  var self = this;

  self.logger.log("AggregateInstantiator", "snapping aggregate root \"" + aggregateRoot.uid +"\" at version " + aggregateRoot.$version);
  var snapshot = Snapshot.makeFromAggregateRoot(aggregateRoot);
  self.store.saveSnapshot(snapshot);
};

module.exports = AggregateInstantiator;