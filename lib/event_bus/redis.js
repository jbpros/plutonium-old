var Emitter  = require("./redis/emitter");
var Receiver = require("./redis/receiver");

var RedisEventBus = {
  Emitter: Emitter,
  Receiver: Receiver
};

module.exports = RedisEventBus;
