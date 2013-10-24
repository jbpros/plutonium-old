function Event(attributes) {
  if (!attributes.name) throw new Error("Missing name");
  if (!attributes.data) throw new Error("Missing data");

  if (!attributes.timestamp)
    attributes.timestamp = Date.now();

  this.uid          = attributes.uid;
  this.name         = attributes.name;
  this.data         = attributes.data;
  this.version      = attributes.version;
  this.replayed     = attributes.replayed;
  this.timestamp    = attributes.timestamp;
  this.aggregateUid = attributes.aggregateUid;
}

module.exports = Event;
