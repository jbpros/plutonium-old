Logger = require "devnull"

logLevel = process.env.LOG_LEVEL ? 6
logger = new Logger timestamp: false, level: logLevel

logger.http = (req, res, next) ->
  logger.log "http", req.method, req.url, req.params or ""
  next()

module.exports = logger
