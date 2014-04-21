
var GoatDB = require('../')
var osenv  = require('osenv')
var path   = require('path')
var tmpdir = osenv.tmpdir()
var pull   = require('pull-stream')
var tape   = require('tape')

var db = GoatDB(path.join(tmpdir, 'test-goatdb1'))

tape('simple', function (t) {

  db.open(function (err) {
    if(err) throw err
    console.log('opened')
    var key = 'foo'
    db.put(key, ''+ new Date(), function (err) {
      if(err) throw err
      db.get(key, function (err, value) {
        if(err) throw err
        console.log(key, '=>', value)
        t.end()
      })
    })
  })

})

tape('bulkload', function (t) {

  db.open(function (err) {
    if(err) throw err

    pull(
      pull.count(999),
      pull.map(function (e) {
        return {key: '#' + Math.random(), value: {count: e, ts: Date.now()}}
      }),
      pull.asyncMap(function (e, cb) {
        db.put(e.key, e.value, cb)
      }),
      pull.drain(null, function (err) {
        if(err) throw err
        console.log('DONE')
      })
    )

  })

})
