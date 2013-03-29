var hasProperty = {}.hasOwnProperty;

function inherit(child, parent) {
  for (var key in parent) {
    if (hasProperty.call(parent, key)) {
      child[key] = parent[key];
    }
  }

  function baseCtor() {
    this.constructor = child;
  }
  baseCtor.prototype = parent.prototype;

  child.prototype = new baseCtor();

  child.super_ = parent;

  return child;
}

module.exports = inherit;
