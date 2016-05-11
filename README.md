# kafka-metamorph
Backwards compatible Kafka consumer for selective partition consumption and explicit offset control

## Background

### TL;DR

With the introduction of Kafka 0.9.0 there are now 3 incompatible consumer interfaces and this project tries to unify them (and provide a consistent API which can be used with several Kafka versions).

See also https://xkcd.com/927/

### The old fashioned way

Before the introduction of version 0.9.0 Apache Kafka provided two consumer interfaces,
the[SimpleConsumer API](http://kafka.apache.org/082/documentation.html#simpleconsumerapi)and the[high-level Consumer API](http://kafka.apache.org/082/documentation.html#highlevelconsumerapi).

While the high-level consumer provided a convenient interface and also took care of offset management, it had a major drawback:
it did not allow explicit offset control (e.g. no possibility to re-process messages) and also it was not possible to consume
only from a subset from partitions.

The SimpleConsumer provided this functionality but in order to use this consumer properly it was neccessary to create a
lot of boilerplate code for error handling and partition faults (see[official Kafka 0.8.0 SimpleConsumer example](https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example)).

Unfortunately both consumer APIs were not compatible and it was rather tedious operation to replace one implementation with the other.

### Brave new world - Kafka 0.9.0

With the release of Kafka 0.9.0 a[new unified consumer API](http://kafka.apache.org/090/documentation.html#consumerapi)was introduced which addressed the issues described above.
This new interface combined the automatic offset management while still providing means to bind only to a selection of partitions
and allowed external offset control.

So why don't we just switch to the Kafka 0.9.0 consumer and stop worrying?

Unfortunately with the release of Kafka 0.9.0 there was also a change within the inter-broker protocol and therefore
the new unified consumer interface cannot be used with older Kafka broker versions.

If you have an infrastructure which is running Kafka brokers with a version lower than 0.9.0 then you are between
a rock and a hard place.

Either you start using one of the deprecated (but still supported) consumer interfaces and you
need to refactor your codebase later or you need to wait until your brokers can be updated to 0.9.x

### Introducing metamorph

This is the part in which kafka-metamorph comes into play. By providing one unified interface which can be
used with older Kafka versions you can implement your use-case today and worry about broker updates later. Yay!