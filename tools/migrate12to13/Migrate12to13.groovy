import java.util.concurrent.*
import v12.journal.io.api.*
import static v12.journal.io.api.Journal.*

srcRoot = this.args[0]
targetRoot = this.args[1]
operations = []
new File(srcRoot).eachDirRecurse { srcDir ->
    relative = srcDir.path.substring(srcRoot.length())
    operations << [srcDir, new File(targetRoot + relative)]
}

threadPool = Executors.newFixedThreadPool(5)
try {
    c = { s, t ->
        println s.path + " -> " + t.path
        
        Journal src = new Journal();
        src.setDirectory(s);
        src.open();
        def target = new journal.io.api.Journal();
        t.mkdirs()
        target.setDirectory(t);
        target.open();
        try {
            for (Location location : src.redo()) {
                byte[] record = src.read(location, ReadType.ASYNC);
                target.write(record, journal.io.api.Journal.WriteType.SYNC)
            }
        } finally {
            src.close()
            target.close()
        }
    }
    
    futures = operations.collect {
        threadPool.submit({-> c(it[0], it[1])} as Callable);    
    }
    futures.each{it.get()}
} finally {
    threadPool.shutdown()
}
