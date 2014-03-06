function Snapshot(attributes) {
  if (!attributes.entityUid) throw new Error("Missing entity UID");
  if (!attributes.version) throw new Error("Missing version");
  if (!attributes.contents) throw new Error("Missing contents");
  this.entityUid = attributes.entityUid;
  this.version   = attributes.version;
  this.contents  = attributes.contents;
}

Snapshot.makeFromEntity = function (entity) {
  var attributes = {
    entityUid: entity.uid,
    version:   entity.$version,
    contents:  entity._serialize()
  };
  var snapshot = new Snapshot(attributes);
  return snapshot;
}

module.exports = Snapshot;