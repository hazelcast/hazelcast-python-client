# Paging Predicate With Custom Comparator

This example shows how to use a paging predicate with a custom
comparator. We will make use of ``HazelcastJsonValue``s and
sort paged responses according to some field of the objects.

Paging is done on the cluster members. Therefore, we have to
write comparison logic in Java, and register the comparator
to the members' config.

On the client side, we will just mark what comparator we will
use and send the data associated with it. Upon receiving it,
cluster members will match the comparator sent from the client
with the one defined in their configuration and use it as the
comparator.

The comparison logic is defined in the 
[AgeComparator](./member-with-comparator/src/main/java/org/example/AgeComparator.java).
The client will just send the Python counterpart of this class
to let the member identify which comparator to use.

To start a Hazelcast member with that comparator defined, you
can use the following ways.

Note that, you would need a Java compiler for both.

## Starting The Member With Maven

If you have [maven](https://maven.apache.org/) installed, you can 
simply run the following command in the ``member-with-comparator``
directory to start the members with the necessary configuration.

```bash
mvn compile exec:java
```

After seeing the member is started, you can run the Python code 
sample.

## Using Hazelcast Distribution

You can also use the Hazelcast distributions to run this code sample.

- Download the distribution from 
[this](https://hazelcast.com/open-source-projects/downloads/) link.
For the scope of this code sample, it is enough to download the slim
distribution.
- Extract the downloaded archive.
- Assuming you are at this directory, run the following command to
compile the Java code.

```bash
javac -cp "/path/to/distribution/lib/*" member-with-comparator/src/main/java/org/example/AgeComparator*.java -d /path/to/distribution/bin/user-lib/
```

- Edit the Hazelcast configuration in the 
``/path/to/distribution/config/hazelcast.xml`` and add the following 
lines to the ``serialization`` tag.

```xml
<hazelcast>
    <serialization>
        <!-- Copy the following to the serialization tag -->
        <data-serializable-factories>
            <data-serializable-factory factory-id="1">
                org.example.AgeComparatorFactory
            </data-serializable-factory>
        </data-serializable-factories>
        <!-- End of copying region -->
    </serialization>
</hazelcast>
```

- Start the member using the ``/path/to/distribution/bin/hz-start`` script.
- After seeing the member is started, you can run the Python code 
sample.
