var fs = require("fs");
var inherit = require('./inherit');

function CommandHandler(Command, Ctor) {
  function Base(command) {}

  Base.getCommandName = function () {
    return Command.getName();
  };

  Base.getCommand = function () {
    return Command;
  };

  inherit(Ctor, Base);
  return Ctor;
}

CommandHandler.registerAllInDirectoryOnBus = function (path, options) {
  if (!options)
    throw new Error("Missing options");
  if (!options.commandBus)
    throw new Error("Missing command bus");
  if (!options.logger)
    throw new Error("Missing logger");

  var commandBus = options.commandBus;
  var logger     = options.logger;
  fs.readdirSync(path).forEach(function (fileName) {
    var CommandHandler = require(path + "/" + fileName);
    logger.log("CommandHandler", "loading command handler for command \"" + CommandHandler.getCommandName() + "\" (" + fileName + ")");
    commandBus.registerCommandHandler(CommandHandler);
  });
};

module.exports = CommandHandler;
