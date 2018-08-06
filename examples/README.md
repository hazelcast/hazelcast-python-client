 Hazelcast Python Client Code Examples Readme
=============================

This folder contains a collection of code samples which you can use to learn how to use Hazelcast features.

How to try these examples
-------------------------

* To try these examples, you should have a running Hazelcast member. If you are already familiar with Hazelcast and have a member running, add its address and port to your configuration as shown in the [**/learning-basics/1-configure_client.py**](learning-basics/1-configure_client.py).
* If not, follow these steps to start an Hazelcast member in your local computer:
    * Make sure that you have [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) installed on your system.
    * Download the latest [Hazelcast release](https://hazelcast.org/download/).
    * Extract the zip file you have downloaded.
    * You should see a directory called **bin** which includes a few scripts and xml files.
    * If you are using Linux/MacOS, you can start a Hazelcast member with **start.sh** and stop the member you have started with **stop.sh** when you are done.
    * If you are using Windows, you can start a Hazelcast member with **start.bat** and stop the member you have started with **stop.bat** when you are done.
    * Refer to the [Using the Scripts In The Package](http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#using-the-scripts-in-the-package) for more information about these scripts.
    * If you use **start.sh/bat** again while your first Hazelcast member is running, you will start a second Hazelcast member. These members will join together to form a Hazelcast cluster.
    * You can increase the size of your cluster by repeating the step above as many times as you want.
    * After successfully running these scripts, you should see the ip address and port of your members on your console. Take a note of these.
    * Add these addresses and ports to your configuration as shown in the [**/learning-basics/1-configure_client.py**](learning-basics/1-configure_client.py). 
* After configuring your client as described above, you are ready to learn using Hazelcast Python Client with these examples.
    
Included example folders
-----------------------

*   **/cloud-discovery** — Includes a code sample that shows how to use Hazelcast.Cloud
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
*   **/ssl** — Contains code samples that show how to configure client with SSL or SSL with mutual authentication
*   **/topic** — Includes a code sample to show usage of distributed topic.
*   **/transactions** — Code samples showing how to use the TransactionalMap.
