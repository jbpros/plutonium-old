function defer(fn) {
  if (typeof(setImmediate) === "function")
    return setImmediate(fn);
  return process.nextTick(fn);
}

module.exports = defer;