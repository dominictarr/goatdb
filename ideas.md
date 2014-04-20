
# read only

In leveldb you may only open an instance in one process...
but you could have other processes open the database in read only mode
easily enough. You could also have each instance create it's own memtable,
(and tail other's memtables... as long as there weren't too many)
then you could have eventual consistency between instances.
Youd need to have timestamps in the memtable, though, + check each memtable
for a key ... which means it would scale the number of processes badly,
but it might be okay for one or two...

# bulk load optimization

when creating a memtable, you could detect whether all the writes are appends.
Then, you could turn that table into a SST. If the file exceeds a given size,
and there has only been appends, treat it like an SST... And even, if you get a non append
write later, just start a new table.

this would mean you could do a bulk load without any compaction. It could basically be the time
to append to a file.
