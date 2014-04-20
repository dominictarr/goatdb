var pull = require('pull-stream')
var para = require('pull-paramap')
var mem = require('../mem')
var createSST = require('../sst')
var compact = require('../compact')
var tape = require('tape')
function generate(db, n, cb) {
  pull(
    pull.count(n - 1),
    pull.asyncMap(function (n, cb) {
      db.put('#'+Math.random(), {count: n}, cb)
    }),
    pull.drain(null, cb)
  )
}

function empty () {
  return function (end, cb) {
    cb(true)
  }
}

tape('sizes are correct', function (t) {

  var db1 = mem()
  generate(db1, 100, function () {

    var db2 = mem()
    console.log('compact1')
    generate(db2, 100, function () {

      compact([db1, db2], createSST, function (err, sst) {
  //      var db3 = mem()

        console.log(sst)
        t.equal(db1.size + db2.size, 200, 'db sizes')
        t.equal(sst.size, 200, 'sst size')

        t.end()
  //      generate(db3, 100, function () {
  //        compact([db3, sst[0]], createSST, function (err, sst2) {
  //
  //          console.log('compact2')
  //          console.log(sst2)
  //        })
  //      })
      })
    })
  })

})