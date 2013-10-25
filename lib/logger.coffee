Logger    = require "devnull"

logLevel = process.env.LOG_LEVEL ? 6
logger = new Logger timestamp: false, level: logLevel

logger.http = (req, res, next) ->
  end = res.end
  req._startTime =  process.hrtime()
  res.end = (chunk, encoding) ->
    status   = res.statusCode
    len      = parseInt(res.getHeader('Content-Length'), 10)
    len      = if isNaN len then "" else "- #{len}"
    diff     = process.hrtime(req._startTime)
    duration = (diff[0] * 1e9 + diff[1]) / 1000000
    line = "#{req.method} #{req.originalUrl} #{res.statusCode} #{duration}ms #{len}"
    logger.log "http", line
    res.end = end
    res.end chunk, encoding
  next()

module.exports = logger
