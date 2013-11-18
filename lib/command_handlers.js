var fs = require("fs");

var CommandHandlers = {
  registerAllInDirectoryOnBus: function registerAllInDirectoryOnBus(path, options) {
    if (!options)
      throw new Error("Missing options");
    if (!options.commandBus)
      throw new Error("Missing command bus");
    if (!options.logger)
      throw new Error("Missing logger");

    var commandBus = options.commandBus;
    var logger     = options.logger;
    fs.readdirSync(path).forEach(function (fileName) {
      var Command = require(path + "/" + fileName);
      logger.log("Commands", "loading command \"" + Command.getName() + "\"");
      commandBus.registerCommandHandler(Command.getName(), Command);
    });
  }
};

module.exports = CommandHandlers;
