
var GoatDB = require('../')
var osenv  = require('osenv')
var path   = require('path')
var tmpdir = osenv.tmpdir()
var pull   = require('pull-stream')
var tape   = require('tape')
var rimraf = require('rimraf')
var cat    = require('pull-cat')

var crypto = require('crypto')

var dir = path.join(tmpdir, 'test-goatdb1')

var input = {}

var db = GoatDB(dir)

function collectObj (cb) {
 return pull.reduce(function (obj, data) {
    obj[data.key] = data.value
    return obj
  }, {}, cb)
}

function bulk (db, cb) {
    var all = []
    var tables = db.snapshot()
    var n = tables.length
    tables.map(function (db, i) {
      return pull(
        db.createReadStream(),
        pull.through(function (data) {
          if(data.key === 'undefined')
            throw new Error('strange: ' + JSON.stringify(data) + ' in ' + db.location)
        }),
        collectObj(function (err, obj) {
          all[i] = obj
          next()
        })
      )
    })

  function next () {
    if(--n) return
    var obj = {}

    all.forEach(function (e) {
      for(var k in e)
        if(!obj[k])
          obj[k] = e[k]
    })

    cb(null, obj, all)
  }
}

function compare(t, a, b) {
  for(var k in a)
    if(!b[k]) t.ok(false, k + ' is extra')
  for(var k in b)
    if(!a[k]) t.ok(false, k + ' is missing')
}

tape('simple', function (t) {
  rimraf(dir, function (err) {
    if(err) throw err
    db.open(function (err) {
      if(err) throw err
      console.log('opened')
      var key = 'foo'
      var value = new Date().toISOString()
      input[key] = {key: key, value: value}
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

function shasum(e) {
  return crypto.createHash('sha256').update(e.toString()).digest('hex')
}

tape('bulkload', function (t) {
  pull(
    pull.count(12345),
    pull.map(function (e) {
      return {key: shasum(e), value: {count: e, ts: Date.now()}}
    }),
    pull.asyncMap(function (e, cb) {
      input[e.key] = e.value
      db.put(e.key, e.value, cb)
    }),
    pull.drain(null, function (err) {
      if(err) throw err

//      bulk(db, function (err, obj, objs) {
//        for(var k in input) {
//          var missing
//          for(var j in objs)
//            if(!objs[j][k])
//              missing = j
//            else {
//              missing = null
//              break
//            }
//          if(missing) {
//            console.log(k, 'is missing from', missing)
//            t.fail(k + ' is missing from ' + missing)
//          }
//        }
//        t.equal(
//          Object.keys(obj).length,
//          Object.keys(input).length
//        )
//        t.end()
//      })

      console.log(
        db.snapshot().map(function (e) { return e.location })
      )

      pull(
        db.createReadStream(),
        pull.collect(function (err, actual) {
          if(err) throw err
          var output = {}
          actual.forEach(function (e) {
            output[e.key] = e.value
          })
          var expected = Object.keys(input).sort().map(function (key) {
            return input[key]
          })
          console.log(Object.keys(output).length, Object.keys(input).length)
          t.equal(Object.keys(output).length, Object.keys(input).length, 'keys length')
          t.equal(actual.length, expected.length)
//          expected.forEach(function (data) {
//            if(!input[data.key])
//              console.log('MISSING', data)
//          })
          t.end()
        })
      )


    })
  )

})
