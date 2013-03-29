function Snapshot(attributes) {
  if (!attributes.aggregateUid) throw new Error("Missing aggregate UID");
  if (!attributes.version) throw new Error("Missing version");
  if (!attributes.contents) throw new Error("Missing contents");
  this.aggregateUid = attributes.aggregateUid;
  this.version      = attributes.version;
  this.contents     = attributes.contents;
}

Snapshot.makeFromAggregateRoot = function (aggregateRoot) {
  var attributes = {
    aggregateUid: aggregateRoot.uid,
    version:      aggregateRoot.$version,
    contents:     aggregateRoot._serialize()
  };
  var snapshot = new Snapshot(attributes);
  return snapshot;
}

module.exports = Snapshot;