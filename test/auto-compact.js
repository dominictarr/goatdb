
var tape = require('tape')
var db = require('../')()
var pull = require('pull-stream')

pull(
  pull.count(1600),
  pull.asyncMap(function (n, cb) {
    db.put('%' + Math.random(), {count: n, value: Math.random()}, cb)
  }),
  pull.drain(null, function (err) {
    console.log(db.snapshot().map(function (e) {
      return { size: e.size }
    }))
  })
)
