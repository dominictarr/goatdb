var compact = require('./compact')
var createSST = require('./sst')
var mem = require('./mem')

module.exports = function (tables) {
  var memtable = mem(), counter = 0, db, compacting = false, _snapshot
  tables = tables || [memtable]
  return db = {
    //get the current snapshot.
    snapshot: function () {
      if(_snapshot) {
        tables = _snapshot; _snapshot = null
      }
      return tables
    },
    //step through all the databases, and look for next
    get: function (key, cb) {
      var tables = db.snapshot()
      ;(function next (i) {
        //if we ran out of tables, err
        //gets need to use snapshots too...
        //but since they will return from the memtable
        //synchronously they will only need to track the SSTs
        //which probably means they can all share a snapshot
        //unless there is a compaction happening.

        //just have an SST snapshot,
        //keep a count of current gets
        //and don't delete any ssts until it's freed.

        if(!tables[i]) return cb(new Error('not found'))
        tables[i].get(key, function (err, value) {
          if(err) return next(i)
          return cb(null, value)
        })
      })(0)
    },
    //only write to the FIRST table.
    put: function (key, value, cb) {
      if(Math.random() < 0.1) console.log(counter)
      return memtable.put(key, value, function (err) {
        if(!(++counter % 200))
          db.compact()
        //db.compact()
        //maybe compact?
        //just check if the number of records added
        cb(err)
      })
    },
    createReadStream: function (opts) {
      //merge streams from all the tables
      //it's probably okay to not have a snapshot on the memtable.
      //hmm, on the other hand, it will be fairly cheap...
      //otherwise I'd need to implement a skiplist, so you can have
      //streams that handle inserts.

      //TRACK HOW MANY ITERATORS ARE USING THIS SNAPSHOT.

      //read snapshots on the memtable are very simple, because it just
      //figures out the range sync and copies it to an array
      //so that doesn't need to be tracked specially.

      var tables = db.snapshot()
      var stream = tables[0].createReadStream(opts)
      for(var i = 1; i < tables.length; i++)
        stream = merge(tables[i].createReadStream(opts), stream)

      return stream
    },
    compact: function (cb) {
      console.log('COMPACT')
      //make a temp snapshot for use while compacting.
      //only one compaction at a time.
      if(compacting) return
      compacting = true
      console.log('COMPACTING...')
      var table = db.snapshot()
      memtable = mem()
      _snapshot = [memtable].concat(tables)
      compact(tables, createSST, function (err, sst, tables) {
      console.log('COMPACTING...DONE')
        compacting = false
        _snapshot = [memtable, sst].concat(tables)
//        console.log(JSON.stringify(_snapshot, null, 2))
        if(cb) cb()
      })
    }
  }
}
