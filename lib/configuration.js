function Configuration(configurationPath, env) {
  var configuration = require(configurationPath)[env];
  if (!configuration)
    throw new Error("No configuration set for environment " + env);
  return configuration;
}

module.exports = Configuration;
