var async        = require("async");
var inherit      = require("./inherit");
var DomainObject = require("./domain_object");

function Entity(name, finalCtor, Ctor) {
  if (!finalCtor) {
    var finalCtor = function() {
      return this.constructor.super_.apply(this, arguments);
    };
  }

  if (!Ctor)
    Ctor = finalCtor;

  var Base = DomainObject(name, finalCtor, function() {
    Base.super_.apply(this, arguments);
    this.uid     = null;
    this.$logger = finalCtor.$logger;
  });

  Base.prototype.toString = function() {
    return "[object Entity:" + this.constructor.name + "]";
  };

  Base.initialize = function() {
    this.$logger = Entity.logger;
  };

  inherit(Ctor, Base);
  return Ctor;
};

Entity.initialize = function(options) {
  if (!options.logger) throw new Error("Missing logger");

  Entity.logger = options.logger;
};

module.exports = Entity;