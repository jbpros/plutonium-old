function BaseEventStore() {
}

BaseEventStore.prototype.setup = function(callback) {
  throw new Error("Implement me");
};

BaseEventStore.prototype.createNewUid = function(callback) {
  throw new Error("Implement me");
};

BaseEventStore.prototype.findAllEvents = function(options, callback) {
  throw new Error("implement me");
};

BaseEventStore.prototype.findAllEventsByAggregateUid = function(aggregateUid, options, callback) {
  throw new Error("Implement me");
};

BaseEventStore.prototype.loadSnapshotForAggregateUid = function(uid, callback) {
  // implement me if you want snapshots in your store
  callback(null, null);
};

BaseEventStore.prototype.saveEvent = function(event, callback) {
  throw new Error("Implement me");
};

BaseEventStore.prototype.saveSnapshot = function(snapshot, callback) {
  // implement me if you want snapshots in your store
  if (callback)
    callback(null);
};

module.exports = BaseEventStore;