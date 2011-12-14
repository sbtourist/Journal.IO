# Journal.IO

Journal.IO is a zero-dependency, fast and easy-to-use journal storage implementation based on append-only rotating logs and checksummed variable-length records, 
supporting concurrent reads and writes, dynamic batching, tunable durability and data compaction.

Journal.IO is a fork of the [HawtJournal](https://github.com/fusesource/hawtjournal) project.

## Quickstart

APIs are very simple and intuitive.

First, create and configure the journal:

    Journal journal = new Journal();
    journal.setDirectory(JOURNAL_DIR);

Then, open the journal:

    journal.open();

And write some records:

    for (int i = 0; i < writes; i++) {
        boolean sync = i % 2 == 0 ? true : false;
        journal.write(new String("DATA" + i), sync);
    }

You can dynamically write either in async or sync mode: in async mode, writes are batched until either the max batch size is reached, 
the journal is manually synced or closed, or a sync write is executed.

Finally, replay the log by going through the Journal "redo" iterable, obtaining record locations:

    for (Location location : journal.redo()) {
        byte[] record = journal.read(location);
        // do something
    }

Eventually delete some record:

    journal.delete(location);

Optionally do a manual sync:

    journal.sync();

Compact logs:

    journal.compact();

And close it:

    journal.close();

That's all!

## About data durability

Journal.IO provides three levels of data durability: batch, sync and physical sync.

Batch durability provides the lowest durability guarantees but the greatest performance: data is collected in memory batches and then written on disk at "sync points",
either when a sync write is explicitly requested, a sync call is explicitly executed, or when the max batch size is reached.

Sync durability happens during sync points, providing higher durability guarantees with some performance cost: here, memory batches are written on disk.

Finally, physical sync durability provides the highest durability guarantees at the expense of a greater performance cost, flushing on disk hardware buffers at every sync point: 
disabled by default, it can be enabled by setting _Journal#setPhysicalSync_ true.

## License

Distributed under the [Apache Software License](http://www.apache.org/licenses/LICENSE-2.0.html).

Journal.IO is based on the HawtJournal project, for original copyright note see: [HawtJournal](https://github.com/fusesource/hawtjournal).