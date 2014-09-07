function Queue(worker, concurrency) {
  var workers = 0;
  var q       = {
    tasks       : [],
    concurrency : concurrency,
    drain       : null,
    push        : function(data) {
      if (!(data instanceof Array))
        data = [data];
      if (data.length == 0 && q.drain)
        setImmediate(q.drain);
      for (i = 0; i < data.length; i++) {
        q.tasks.push(data[i]);
        setImmediate(q.process);
      }
    },
    process     : function() {
      if (workers < q.concurrency && q.tasks.length) {
        var task = q.tasks.shift();
        workers++;
        var next = function () {
          workers--;
          if (q.drain && q.tasks.length + workers === 0)
            q.drain();
          setImmediate(q.process);
        };
        worker(task, next);
      }
    },
    length: function () {
      return q.tasks.length;
    }
  };
  return q;
}

module.exports = Queue;
