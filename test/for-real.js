
var GoatDB = require('../')
var osenv  = require('osenv')
var path   = require('path')
var tmpdir = osenv.tmpdir()
var pull   = require('pull-stream')
var tape   = require('tape')
var rimraf = require('rimraf')

var dir = path.join(tmpdir, 'test-goatdb1')


var input = []

var db = GoatDB(dir)

tape('simple', function (t) {

  rimraf(dir, function (err) {
    if(err) throw err
    db.open(function (err) {
      if(err) throw err
      console.log('opened')
      var key = 'foo'
      var value = new Date().toISOString()
      input.push({key: key, value: value})
      db.put(key, value, function (err) {
        if(err) throw err
        db.get(key, function (err, value) {
          if(err) throw err
          console.log(key, '=>', value)
          t.end()
        })
      })
    })
  })

})

tape('bulkload', function (t) {


  pull(
    pull.count(256),
    pull.map(function (e) {
      return {key: '#' + Math.random(), value: {count: e, ts: Date.now()}}
    }),
    pull.asyncMap(function (e, cb) {
      input.push(e)
      db.put(e.key, e.value, cb)
    }),
    pull.drain(null, function (err) {
      if(err) throw err
      console.log('DONE')
      pull(
        db.createReadStream(),
        pull.collect(function (err, actual) {
          if(err) throw err
          input.sort(function (a, b) {
            return a.key < b.key ? -1 : a.key > b.key ? 1 : 0
          })
          t.deepEqual(actual, input)
          t.end()
        })
      )
    })
  )

})
