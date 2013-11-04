function _padLeft(str, length) {
  str = str.toString();
  while (str.length < length) {
    str = " " + str;
  }
  return str;
};

function Profiler(subject, logger) {
  if (!logger) throw new Error("Missing logger");

  this.logger  = logger;
  this.subject = subject;
}

Profiler.prototype.start = function(subject) {
  this.startTime = process.hrtime();
};

Profiler.prototype.end = function(options) {
  if (!options)
    var options = {};
  var diff   = process.hrtime(this.startTime);
  this.spent = (diff[0] * 1e9 + diff[1]) / 1000000;

  if (!options.silent) {
    var spent = "" + (_padLeft(this.spent, 13)) + "ms";
    return this.logger.debug("profiler", "" + spent + " on " + this.subject);
  }
};

module.exports = Profiler;
