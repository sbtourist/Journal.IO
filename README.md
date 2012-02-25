# Journal.IO

Journal.IO is a zero-dependency, fast and easy-to-use journal storage implementation based on append-only rotating logs and checksummed variable-length records, 
supporting concurrent reads and writes, dynamic batching, tunable durability and data compaction.

Journal.IO has been forked from the [HawtJournal](https://github.com/fusesource/hawtjournal) project, 
in order to provide faster development and release cycles, as well as foster open collaboration.

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
        journal.write(new String("DATA" + i), WriteType.SYNC);
    }

You can dynamically write either in async (_WriteType.ASYNC_) or sync mode (_WriteType.SYNC_): 
in async mode, writes are batched until either the max batch size is reached, 
the journal is manually synced or closed, or a sync write is executed.

The, forward-replay the log by going through the Journal "redo" iterable, obtaining record locations by reading them either in
async (_ReadType.ASYNC_) or sync mode (_ReadType.SYNC_): async mode takes advantage of speculative reads and so it's faster than sync mode, 
which instead is slower but able to suddenly detect deleted records:

    for (Location location : journal.redo()) {
        byte[] record = journal.read(location, ReadType.SYNC);
        // do something
    }

You can also backward-replay the log by going through the Journal "undo" iterable:

    for (Location location : journal.undo()) {
        byte[] record = journal.read(location, ReadType.SYNC);
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

## Download

Journal.IO is a self-contained jar you can download from the Downloads section above.

If you're a Maven user, you can also configure it on your project by importing the repository:

    <repository>
        <id>Journal.IO</id>
        <url>https://raw.github.com/sbtourist/Journal.IO/master/m2/repo</url>
    </repository>

And then declaring the dependency:

    <dependency>
        <groupId>journalio</groupId>
       <artifactId>journalio</artifactId>
       <version>1.0</version>
    </dependency>

## Feedback

Join the mailing list at: http://groups.google.com/group/journalio 
Or feel free to contact [me](http://www.twitter.com/sbtourist) on Twitter.

## License

Distributed under the [Apache Software License](http://www.apache.org/licenses/LICENSE-2.0.html).

Journal.IO is based on the HawtJournal project, for original copyright note see: [HawtJournal](https://github.com/fusesource/hawtjournal).

## Developers

[Sergio Bossa](http://www.twitter.com/sbtourist) (Journal.IO lead).
[Hiram Chirino](http://www.twitter.com/hiramchirino) (Original author of the HawtJournal project).
[Chris Vest](http://www.twitter.com/chvest) (Contributor)