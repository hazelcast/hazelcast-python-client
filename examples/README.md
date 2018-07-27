 Hazelcast Python Client Code Examples Readme
=============================

This folder contains a collection of code samples which you can use to learn how to use Hazelcast features.

How to try examples
----------
* If you have a running Hazelcast instance on the server, add its address and port to your configuration as shown in the [**/learning-basics/1-configure_client.py**](learning-basics/1-configure_client.py). 
* If you want run these examples in your local computer that does not have a running hazelcast instance, [download the latest hazelcast release](https://hazelcast.org/download/) and start an instance as described in [getting started section](http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#using-the-scripts-in-the-package). You should see the address and port of your instance on your console. Then, add its address and port to your configuration as shown in the [**/learning-basics/1-configure_client.py**](learning-basics/1-configure_client.py).  
* After configuring your client as described above, you are ready to learn using Hazelcast Python Client with these examples.
    
Included example folders
-----------------------

*   **/learning-basics** — Code samples to show some Hazelcast basics like creating, configuring and destroying Hazelcast instances, configuring logging and Hazelcast configuration.
*   **/list** — Contains a code sample to show usage of distributed list.
*   **/map** — Code samples folder that includes some features of the Hazelcast distributed map in action.
*   **/monitoring** — Includes code samples that show how to check status of Hazelcast instances using listeners.
*   **/multimap** — Contains a code sample to show usage of distributed multi map.
*   **/org-website** — Contains code samples that are shown on the https://hazelcast.org.
*   **/queue** — Includes a code sample to show usage of distributed queue.
*   **/ring-buffer** — Contains a code sample to show usage of distributed ring buffer.
*   **/serialization** — Includes code samples that implement various serialization interfaces like IdentifiedDataSerializable, Portable. It also has code samples to show how to plug a custom and global serializers using StreamSerializer.
*   **/set** — Contains a code sample to show usage of distributed set.
*   **/topic** — Includes a code sample to show usage of distributed topic.
*   **/transactions** — Code samples showing how to use the TransactionalMap.