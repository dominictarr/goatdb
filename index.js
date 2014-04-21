
var inject         = require('./inject')
var createLog      = require('json-logdb')
var createSST      = require('json-sst')
var createManifest = require('kiddb')

module.exports = inject(createSST, createLog, createManifest)
