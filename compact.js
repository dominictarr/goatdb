var merge = require('pull-merge')
var pull = require('pull-stream')
var sst = require('./sst')

function sizes (tables) {
  return tables.map(function (e) { return { level: e.level, size: e.size, type: e.type} })
}

module.exports = function (tables, createSST, cb) {
  var tables = tables.slice()
  var total = tables.reduce(function (a, e) { return a + e.size }, 0)

  var memtable = tables.shift()
  var stream = memtable.createReadStream(), level = 1, levels = [1]
  var target = 100, level = 0

  //estimates for the final size of the compaction.
  //if there are lots of collisions, then it's the size of the largest table
  //if there are no collisions, then it's all the sizes added together.

  var max = 0, min = memtable.size

  while(tables.length) {
    var table = tables[0]
    if(table.size > target * 2)
      break //compact this table
    else if(table.size <= target * 2) {
      target *= 2
      console.log('double', target)
    }

    max += table.size
    min = Math.max(min, table.size)
    tables.shift()

    stream = merge(table.createReadStream(), stream, function (a, b) {
      return a.key < b.key ? -1 : a.key > b.key ? 1 : 0
    })
  }

  pull(
    stream,
    createSST(function (err, sst) {
      console.log('COMPACTED', sizes([sst].concat(tables)))

      cb(null, sst, tables)
    })
  )
}

