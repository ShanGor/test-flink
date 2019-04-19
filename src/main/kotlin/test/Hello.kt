package test

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Implements a streaming windowed version of the "WordCount" program.
 *
 *
 * This program connects to a server socket and reads strings from the socket.
 * The easiest way to try this out is to open a text server (at port 12345)
 * using the *netcat* tool via
 * <pre>
 * nc -l 12345
</pre> *
 * and run this example with the hostname and the port as arguments.
 */

fun main(args: Array<String>) {

    // the host and the port to connect to
    val hostname: String
    val port: Int
    try {
        val params = ParameterTool.fromArgs(args)
        hostname = if (params.has("hostname")) params.get("hostname") else "localhost"
        port = params.getInt("port")
    } catch (e: Exception) {
        System.err.println(
            "No port specified. Please run 'SocketWindowWordCount " +
                    "--hostname <hostname> --port <port>', where hostname (localhost by default) " +
                    "and port is the address of the text server"
        )
        System.err.println(("To start a simple text server, run 'netcat -l <port>' and " + "type the input text into the command line"))
        return
    }

    // get the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    // get input data by connecting to the socket
    val text = env.socketTextStream(hostname, port, "\n")

    // parse the data, group it, window it, and aggregate the counts
    val windowCounts = text

        .flatMap(FlatMapFunction<String, WordWithCount> { value, out ->
            for (word in value.split(("\\s").toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()) {
                out.collect(WordWithCount(word, 1L))
            }
        })
        .keyBy(KeySelector<WordWithCount, String> {it.word}) // Must keep the KeySelector here. otherwise would get error.
        .timeWindow(Time.seconds(5))
        .reduce { a, b -> WordWithCount(a.word, a.count + b.count) }

    // print the results with a single thread, rather than in parallel
    windowCounts.print().setParallelism(1)

    env.execute("Socket Window WordCount")
}

// ------------------------------------------------------------------------

/**
 * Data type for words with count. Currently, if dont implement these methods, will get error. cannot treat as key.
 * The keys and the values should be var instead of val.
 */
data class WordWithCount(var word: String, var count: Long): Comparable<WordWithCount> {
    override fun compareTo(other: WordWithCount): Int {
        return this.toString().compareTo(other.toString())
    }

    override fun hashCode(): Int {
        return toString().hashCode()
    }

    override fun toString(): String {
        return "$word: $count"
    }
}

