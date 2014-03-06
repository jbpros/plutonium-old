var Entity = require('./entity');
var inherit = require('./inherit');

function AggregateRoot(name, finalCtor, Ctor) {
  if (!finalCtor) {
    finalCtor = function () {
      this.constructor.super_.apply(this, arguments);
    };
  }

  if (!Ctor) Ctor = finalCtor;

  var Base = Entity(name, finalCtor, function () {
    Base.super_.apply(this, arguments);
    return this;
  });

  Base.prototype.toString = function () {
    return "[object AggregateRoot:#{@constructor.name} <uid: #{@uid}>]";
  };

  inherit(Ctor, Base);
  return Ctor;
};

AggregateRoot.initialize = function (options) {
  if (!options.logger) throw new Error("Missing logger");
  AggregateRoot.logger = options.logger;
};

module.exports = AggregateRoot;
