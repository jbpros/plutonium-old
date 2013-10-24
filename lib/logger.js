var Logger   = require("devnull");
var logLevel = process.env.LOG_LEVEL != null ? process.env.LOG_LEVEL : 6;

var logger = new Logger({
  timestamp: false,
  level: logLevel
});

logger.http = function(req, res, next) {
  var end        = res.end;
  req._startTime = process.hrtime();

  res.end = function(chunk, encoding) {
    var status    = res.statusCode;
    var len       = parseInt(res.getHeader("Content-Length"), 10);
    len           = isNaN(len) ? "" : "- " + len;

    var diff      = process.hrtime(req._startTime);
    var duration  = (diff[0] * 1e9 + diff[1]) / 1000000;
    var line      = "" + req.method + " " + req.originalUrl + " " + res.statusCode + " " + duration + "ms " + len;
    logger.log("http", line);

    res.end = end;
    res.end(chunk, encoding);
  };
  next();
};

module.exports = logger;