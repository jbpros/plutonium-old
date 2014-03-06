var inherit = require('./inherit');

function Command(name, Ctor) {
  if (!Ctor) {
    Ctor = function () {
      this.constructor.super_.apply(this, arguments);
    }
  }

  function Base(payload) {
    for (var property in payload) {
      this[property] = payload[property];
    }
  }

  Base.toString = function () { return "[object DomainObject:" + name + "]"; };

  Base.super = function () {
    this.constructor.super_.apply(this, arguments);
  };

  Base.getName = function () {
    return name;
  };

  Base.prototype.getName = function () {
    return name;
  };

  Base.prototype.getPayload = function () {
    var serialized = {};
    for (var property in this) {
      if (this.hasOwnProperty(property))
        serialized[property] = this[property];
    };
    return serialized;
  };

  inherit(Ctor, Base);
  return Ctor;
}

module.exports = Command;
