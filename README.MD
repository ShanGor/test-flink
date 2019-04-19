# Test Flink (Word Count)

Use Kotlin to write a simple Flink application. Build with Maven and can be run inside the Flink.

#### Why create this?
Because the sample case in the official website, is hard to compile. You need to download the whole source package. ([Link](https://ci.apache.org/projects/flink/flink-docs-release-1.8/tutorials/local_setup.html))

So I create this project, to reduce the dependencies to the whole sample projects.

#### How to run.

```
$ mvn clean install

Run below command and input some words to be count.
$ nc -l 9000

$ flink run target/test-flink-1.0-SNAPSHOT.jar --port 9000

Press CTRL+D at the nc command after pressed some words.
```

Check the result at the log folder. In MAC:
```
cat /usr/local/Cellar/apache-flink/1.8.0/libexec/log/*.out
```