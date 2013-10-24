function Report(attributes) {
  this.attributes = attributes;
}

Report.prototype.get = function(attributeName) {
  return this.attributes[attributeName];
};

Report.prototype.set = function(attributeName, value) {
  this.attributes[attributeName] = value;
};

Report.prototype.getAttributes = function() {
  return this.attributes;
};

Report.prototype.toJSON = function(options) {
  if (!options)
    options = {};
  var attributes = {};
  for (var key in this.attributes) {
    var value = this.attributes[k];
    if (!(value instanceof Buffer))
      attributes[key] = value;
  }
  return attributes;
};

Report.prototype.inspect = function() {
  return this.toString();
};

Report.prototype.toString = function() {
  var attributes = [];
  for (var key in this.attributes) {
    var value = this.attributes[key];
    if (!(value instanceof Buffer)) {
      attributes.push("" + key + "=\"" + value + "\"");
    }
  }
  return "[reportObject " + this.constructor.name + " <" + (attributes.join(', ')) + ">]";
};

module.exports = Report;
