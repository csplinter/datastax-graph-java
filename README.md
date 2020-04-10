# datastax-graph-java
Small example showing DataStax Graph usage with Java Driver 3.x and 4.x

Running against DataStax Enterprise 6.8 Graph

```
cd java-driver-4x; mvn clean compile exec:java -Dexec.mainClass=ExampleNew
```

Running against DataStax Enterprise older versions of Graph 

```
cd java-driver-4x; mvn clean compile exec:java -Dexec.mainClass=ExampleOld
```

* Note the advanced.graph.sub-protocol = graphson-1.0 in [application.conf](java-driver-4x/src/main/resources/application.conf) *