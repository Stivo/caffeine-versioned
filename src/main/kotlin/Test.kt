import com.github.benmanes.caffeine.cache.AsyncCacheLoader
import com.github.benmanes.caffeine.cache.AsyncLoadingCache
import com.github.benmanes.caffeine.cache.Caffeine
import java.lang.StringBuilder
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.function.Function
import java.util.function.Supplier
import java.util.stream.IntStream.range
import kotlin.collections.ArrayList
import kotlin.streams.toList


fun main(args: Array<String>) {
    val dbLoader = DbLoader()
    val loader = BulkLoader(dbLoader)
    val ex = Executors.newFixedThreadPool(10)
    val cache = Caffeine.newBuilder()
            .maximumSize(100000)
            .executor(ex)
            .buildAsync(loader)
    loader.cache = cache
    val ints = arrayOf(45, 46, 47, 48, 49, 50, 50, 1000, 1000)
    val threads = ArrayList<Thread>()
    for (int in range(1, 21)) {
        val loaderThread = LoaderThread("Thread $int", ints, dbLoader, cache)
        loaderThread.start()
        threads.add(loaderThread)
    }
    for (thread in threads) {
        thread.join()
    }
    ex.shutdown()
}

class LoaderThread(val prefix: String, val ints: Array<Int>, val dbLoader: DbLoader, val cache: AsyncLoadingCache<Key, Value>) : Thread() {
    val keys = range(1, 1000).mapToObj { e -> "$prefix: $e" }.toList()

    init {
        isDaemon = true
    }

    override fun run() {
        for (int in ints) {
            val dbLoadsBefore = dbLoader.loads
            val start = System.currentTimeMillis()
            val get = cache.getAll(keys.map { e -> Key(e, int) }).get()
            val sb = StringBuilder()
            sb.append("Looking up $int: $get\n")
            sb.append("Took: ${System.currentTimeMillis() - start} ms\n")
            sb.append("Loaded from db ${dbLoader.loads} times\n")
            sb.append("Active threads: ${Thread.activeCount()}\n")
            sb.append("Active threads: ${Thread.activeCount()}\n")
            sb.append("Amount of keys in cache: ${cache.synchronous().asMap().size}\n")
            sb.append("Actual stored values in memory: ${cache.synchronous().asMap().values.toSet().size}\n")
            sb.append("Minimum version: ${cache.synchronous().asMap().minBy { e -> e.key.version }}\n")
            sb.append("Entries to be evicted: ${cache.synchronous().policy().eviction().get().coldest(100)}\n")
            sb.append("=======================================================================\n")
            println(sb)
        }

    }
}

data class Key(val key: String, val version: Int)

open class Value(val key: String, val creationVersion: Int) {
    override fun toString(): String {
        return "Value(key='$key', creationVersion=$creationVersion)"
    }
}

object NullValue : Value("", -1)

class BulkLoader(val dbLoader: DbLoader) : AsyncCacheLoader<Key, Value> {
    lateinit var cache: AsyncLoadingCache<Key, Value>
    override fun asyncLoad(key: Key, executor: Executor): CompletableFuture<Value> {
        return asyncLoadAll(Arrays.asList(key), executor).thenApplyAsync(Function { e -> e[key] }, executor)
    }

    override fun asyncLoadAll(keys: MutableIterable<Key>, executor: Executor): CompletableFuture<MutableMap<Key, Value>> {
        val loadFromDb = CompletableFuture.supplyAsync(Supplier {
            val keyList = keys.toList()
            val fromDb = dbLoader.getAll(keyList)
            Pair(keyList, fromDb)
        }, executor)
        val loadFromCache: (Pair<List<Key>, MutableMap<Key, Value>>) -> CompletableFuture<MutableMap<Key, Value>> = { pair ->
            val fromDb = pair.second
            val keyList = pair.first
            val keysToLoad = keys.toMutableSet()
            keysToLoad.removeAll(fromDb.keys)
            if (keysToLoad.isEmpty() || (keyList.isNotEmpty() && keyList[0].version == 1)) {
                asFuture(fromDb)
            } else {
                val version = keyList[0].version
                val from = cache.getAll(keysToLoad.map { e -> Key(e.key, e.version - 1) })
                from
                        .thenApplyAsync {
                            fromDb.putAll(it.mapKeys { e -> Key(e.key.key, version) })
                            fromDb
                        }
            }

        }
        return loadFromDb
                .thenComposeAsync(loadFromCache)

    }

    private fun <T> asFuture(key: T): CompletableFuture<T> {
        return CompletableFuture.completedFuture(key)
    }

}

class DbLoader {
    var loads = 0
    private val r = Random()
    fun getAll(keys: List<Key>): MutableMap<Key, Value> {
        val out = HashMap<Key, Value>()
        for (key in keys) {
            if (r.nextInt(100) < 10) {
                out[key] = Value("Some key", creationVersion = key.version)
            } else if (key.version == 1) {
                out[key] = NullValue
            }
        }
        Thread.sleep(10)
        loads++
        return out
    }
}
