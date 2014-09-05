function Profiler(subject, logger) {
  if (!logger) throw new Error("Missing logger");
  this.subject = subject;
  this.logger = logger;
}

Profiler.prototype.start = function(subject) {
  return this.startTime = process.hrtime();
};

Profiler.prototype.end = function(_arg) {
  var silent = (_arg != null ? _arg : {}).silent;
  var diff = process.hrtime(this.startTime);
  this.spent = (diff[0] * 1e9 + diff[1]) / 1000000;
  if (!silent) {
    var spent = "" + (_padLeft(this.spent, 13)) + "ms";
    return this.logger.debug("profiler", "" + spent + " on " + this.subject);
  }
};

function _padLeft(str, length) {
  str = str.toString();
  while (str.length < length) {
    str = " " + str;
  }
  return str;
};

module.exports = Profiler;
