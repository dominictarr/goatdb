var compact = require('./compact')
var createSST = require('./sst')
var mem = require('./mem')
var zeros = '00000000'
function pad (n) {
  n = n.toString()
  return zeros.substring(0, n.length) + n
}

module.exports = function (createSST, createMemtable, createManifest) {

  return function (location, opts) {
    var memtable = mem(), counter = 0, db, compacting = false, _snapshot
    var tables = tables || [memtable]
    var manifest, tables, seq
    return db = {
      //should i separate all this stuff out so that
      //you can simulate the database all in memory?
      open: function (opts, cb) {
        if(!cb) cb = opts, opts = {}
        mkdirp(location, function (err) {
          if(err) return cb(err)
          manifest = creatManifest(path.join(location, 'manifest.json'))
          manifest.open(function (err) {
            if(err) return cb(err)
            //if the manifest is empty create an empty memory table.
            if(!manifest.data) {
              seq = 0
              var filename = 
              var _memtable = createMemtable(path.join(location, 'log-'+pad(seq)+'.json'))
              manifest.update({tables: ['log-'+pad(seq)+'.json'], seq: seq}, function (err) {
                if(err) return cb(err)
                _memtable.open(function (err) {
                  if(err) cb(err)
                  memtable = _memtable
                  _snapshot = [memtable]
                  cb(null, db)
                })
              })
            } else {
              //open all the tables.
              var _tables = []
              var n = manifest.tables.length
              manifest.tables.forEach(function (name, i) {
                var m = /^(log|sst)-(\d+)\.json$/.exec(name)
                var type = m[1], _seq = m[2], table
                var create = type == 'log' ? createMemtable : createSST
                var table = tables[i] = create(name)
                table.open(next)
              })
              function next (err) {
                if(err) return n = -1, cb(err)
                if(--n) return
                cb(null, db)
              }
            }
          })
        }
      },
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
        // create a new memtable, to save any writes while we are compacting.
        // save it in the manifest FIRST. That way we can be sure that we don't
        // loose that data if we crash while compacting.
//        manifest.put('tables', 

        //add a new memtable to the manifest
        var name = nextTamleName('log')
        var tables = db.snapshot()
        var names = tables.map(function (e) {
          return path.basename(e.location)
        })
        names.unshift(name)
        manifest.update({tables: names, seq: seq}, function (err) {

          _snapshot = [memtable].concat(tables)
          var table = createMemtable(path.join(location, name).open(function ()

          console.log('COMPACTING...')
          memtable =  mem(nextTableName('log'))
          compact(tables, createSST, function (err, sst, tables) {
          console.log('COMPACTING...DONE')
            compacting = false
            //now that we have generated the sst,
            //save the new table set in the manifest,
            //so that if the process crashes we will reload the right data.
            //hmm, maybe we could just start using it... sst table are immutable, anyway.
            //the most important thing to have right is the memtables.
            //when an sst is no longer used (been compacted + no more iterators)
            //then it's safe to delete.
            _snapshot = [memtable, sst].concat(tables)
            if(cb) cb()
          })
        })
      }
    }
  }
}
