Logger = require "devnull"

logLevel = process.env.LOG_LEVEL ? 6
logger = new Logger timestamp: false, level: logLevel

logger.http = (req, res, next) ->
  end = res.end
  req._startTime = new Date
  res.end = (chunk, encoding) ->
    status   = res.statusCode
    len      = parseInt(res.getHeader('Content-Length'), 10)
    duration = new Date - req._startTime;
    line = "#{req.method} #{req.originalUrl} #{res.statusCode} #{duration}ms #{len}"
    logger.log "http", line
    res.end = end
    res.end chunk, encoding
  next()

module.exports = logger
