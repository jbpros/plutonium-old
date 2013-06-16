Logger    = require "devnull"
microtime = require "microtime-x"

logLevel = process.env.LOG_LEVEL ? 6
logger = new Logger timestamp: false, level: logLevel

logger.http = (req, res, next) ->
  end = res.end
  req._startTime = microtime.now()
  res.end = (chunk, encoding) ->
    status   = res.statusCode
    len      = parseInt(res.getHeader('Content-Length'), 10)
    len      = if isNaN len then "" else "- #{len}"
    duration = (microtime.now() - req._startTime) / 1000000
    line = "#{req.method} #{req.originalUrl} #{res.statusCode} #{duration}s #{len}"
    logger.log "http", line
    res.end = end
    res.end chunk, encoding
  next()

module.exports = logger
