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
  var self = this;

  var entity = DomainObject.deserialize(snapshot.contents);
  var uid       = snapshot.entityUid;
  var version   = snapshot.version;
  entity._initializeAtVersion(version);

  self.logger.log("EntityInstantiator", "looking for events of entity \""+ uid + "\" after version " + version);
  self.store.findAllEventsByEntityUidAfterVersion(uid, version, function (err, events) {
    if (err) return callback(err);
    self.logger.log("EntityInstantiator", "found " + events.length + " event(s) of entity \""+ uid + "\" after version " + version);
    entity.applyEvents(events, function (err) {
      if (err) return callback(err);
      var snapshotAge = entity.$version - version;
      if (snapshotAge > SNAPSHOT_MAX_AGE) {
        self.logger.log("EntityInstantiator", "updating outdated snapshot (" + snapshotAge + " versions) of entity \""+ uid + "\"");
        self._snapEntity(entity);
      }
      self.logger.log("EntityInstantiator", "instantiated entity \""+ uid + "\" from snapshot at version " + version);
      callback(null, entity);
    });
  });
};

EntityInstantiator.prototype._instantiateThroughEventsFromUid = function (uid, callback) {
  var self = this;

  self.store.findAllEventsByEntityUid(uid, function (err, events) {
    if (err) {
      callback(err);
    } else if (events.length == 0) {
      callback(null, null);
    } else {
      self.Entity.buildFromEvents(events, function (err, entity) {
        if (err) return callback(err);
        if (entity.$version > SNAPSHOT_MAX_AGE)
          self._snapEntity(entity);
        self.logger.log("EntityInstantiator", "instantiated entity \""+ uid + "\" at version " + entity.$version + " from " + events.length + " events");
        callback(null, entity);
      });
    }
  });
};

EntityInstantiator.prototype._snapEntity = function (entity) {
  var self = this;

  self.logger.log("EntityInstantiator", "snapping entity \"" + entity.uid +"\" at version " + entity.$version);
  var snapshot = Snapshot.makeFromEntity(entity);
  self.store.saveSnapshot(snapshot);
};

module.exports = EntityInstantiator;
