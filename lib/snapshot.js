function Snapshot(attributes) {
  if (!attributes.version)      throw new Error("Missing version");
  if (!attributes.contents)     throw new Error("Missing contents");
  if (!attributes.aggregateUid) throw new Error("Missing aggregate UID");

  this.version      = attributes.version;
  this.contents     = attributes.contents;
  this.aggregateUid = attributes.aggregateUid;
}

Snapshot.makeFromAggregateRoot = function (aggregateRoot) {
  var attributes = {
    version      : aggregateRoot.$version,
    contents     : aggregateRoot._serialize(),
    aggregateUid : aggregateRoot.uid
  };
  var snapshot = new Snapshot(attributes);
  return snapshot;
}

module.exports = Snapshot;