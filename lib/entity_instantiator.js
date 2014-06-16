var DomainObject = require("./domain_object");
var Snapshot     = require("./snapshot");

var SNAPSHOT_MAX_AGE = 99;

// todo: use one instance all the time?
function EntityInstantiator(options) {
  if (!options.store) throw new Error("Missing store");
  if (!options.Entity) throw new Error("Missing entity constructor");
  if (!options.logger) throw new Error("Missing logger");
  this.store  = options.store;
  this.Entity = options.Entity;
  this.logger = options.logger;
}

EntityInstantiator.prototype.findByUid = function (uid, callback) {
  var self = this;
  self.store.loadSnapshotForEntityUid(uid, function (err, snapshot) {
    if (err) return callback(err);

    if (snapshot) {
      self._instantiateFromSnapshot(snapshot, callback);
    } else {
      self._instantiateThroughEventsFromUid(uid, callback);
    }
  });
};

EntityInstantiator.prototype._instantiateFromSnapshot = function (snapshot, callback) {
  var self      = this;
  var entity    = DomainObject.deserialize(snapshot.contents);
  var uid       = snapshot.entityUid;
  var version   = snapshot.version;

  entity._initializeAtVersion(version);

  var eventHandler = function(event, callback) {
    entity.applyEvent(event, callback);
  };

  self.store.iterateOverEntityEventsAfterVersion(uid, version, eventHandler, function (err) {
    if (err) return callback(err);

    self.logger.log("EntityInstantiator", "instantiated entity \""+ uid + "\" from snapshot at version " + version);
    callback(null, entity);

    var snapshotAge = entity.$version - version;

    self._snapEntityIfNeeded(entity, snapshotAge, function(err) {
      if (err) self.logger.error("EntityInstantiator", "Snap entity failed", err);
    });
  });
};

EntityInstantiator.prototype._instantiateThroughEventsFromUid = function (uid, callback) {
  var self   = this;
  var entity = new self.Entity();

  var eventHandler = function(event, callback) {
    entity.applyEvent(event, callback);
  };

  self.store.iterateOverEntityEvents(uid, eventHandler, function (err) {
    if (err) return callback(err);

    if (entity.$version === 0)
      callback(null, null);
    else {
      self.logger.log("EntityInstantiator", "instantiated entity \"" + uid + "\" at version " + entity.$version);
      callback(null, entity);

      self._snapEntityIfNeeded(entity, entity.$version, function(err) {
        if (err) self.logger.error("EntityInstantiator", "Snap entity failed", err);
      });
    }
  });
};

EntityInstantiator.prototype._snapEntityIfNeeded = function (entity, snapshotAge, callback) {
  if (snapshotAge > SNAPSHOT_MAX_AGE) {
    this.logger.log("EntityInstantiator", "updating snapshot (" + snapshotAge + " versions) of entity \"" + entity.uid + "\"");
    this._snapEntity(entity, callback);
  } else
    callback();
};

EntityInstantiator.prototype._snapEntity = function (entity, callback) {
  var self = this;

  self.logger.log("EntityInstantiator", "snapping entity \"" + entity.uid +"\" at version " + entity.$version);
  var snapshot = Snapshot.makeFromEntity(entity);
  self.store.saveSnapshot(snapshot, function(err) {
    if (err) return callback(err);
    callback();
  });
};

module.exports = EntityInstantiator;
