# Table of Contents

* [Introduction](#introduction)
* [1. Getting Started](#1-getting-started)
  * [1.1. Requirements](#11-requirements)
  * [1.2. Working with Hazelcast IMDG Clusters](#12-working-with-hazelcast-imdg-clusters)
    * [1.2.1. Setting Up a Hazelcast IMDG Cluster](#121-setting-up-a-hazelcast-imdg-cluster)
      * [1.2.1.1. Running Standalone JARs](#1211-running-standalone-jars)
      * [1.2.1.2. Adding User Library to CLASSPATH](#1212-adding-user-library-to-classpath)
  * [1.3. Downloading and Installing](#13-downloading-and-installing)
  * [1.4. Basic Configuration](#14-basic-configuration)
    * [1.4.1. Configuring Hazelcast IMDG](#141-configuring-hazelcast-imdg)
    * [1.4.2. Configuring Hazelcast Python Client](#142-configuring-hazelcast-python-client)
      * [1.4.2.1. Group Settings](#1421-group-settings)
      * [1.4.2.2. Network Settings](#1421-network-settings)
    * [1.4.3. Client System Properties](#143-client-system-properties)
  * [1.5. Basic Usage](#15-basic-usage)
  * [1.6. Code Samples](#16-code-samples)
* [2. Features](#2-features)
* [3. Configuration Overview](#3-configuration-overview)
  * [3.1. Configuration Options](#31-configuration-options)
    * [3.1.1. Programmatic Configuration](#311-programmatic-configuration)
* [4. Serialization](#4-serialization)
  * [4.1. IdentifiedDataSerializable Serialization](#41-identifieddataserializable-serialization)
  * [4.2. Portable Serialization](#42-portable-serialization)
  * [4.3. Custom Serialization](#43-custom-serialization)
  * [4.4. JSON Serialization](#44-json-serialization)
  * [4.5. Global Serialization](#45-global-serialization)
* [5. Setting Up Client Network](#5-setting-up-client-network)
  * [5.1. Providing Member Addresses](#51-providing-member-addresses)
  * [5.2. Setting Smart Routing](#52-setting-smart-routing)
  * [5.3. Enabling Redo Operation](#53-enabling-redo-operation)
  * [5.4. Setting Connection Timeout](#54-setting-connection-timeout)
  * [5.5. Setting Connection Attempt Limit](#55-setting-connection-attempt-limit)
  * [5.6. Setting Connection Attempt Period](#56-setting-connection-attempt-period)
  * [5.7. Enabling Client TLS/SSL](#57-enabling-client-tlsssl)
  * [5.8. Enabling Hazelcast Cloud Discovery](#58-enabling-hazelcast-cloud-discovery)
* [6. Securing Client Connection](#6-securing-client-connection)
  * [6.1. TLS/SSL](#61-tlsssl)
    * [6.1.1. TLS/SSL for Hazelcast Members](#611-tlsssl-for-hazelcast-members)
    * [6.1.2. TLS/SSL for Hazelcast Python Clients](#612-tlsssl-for-hazelcast-python-clients)
    * [6.1.3. Mutual Authentication](#613-mutual-authentication)
* [7. Using Python Client with Hazelcast IMDG](#7-using-python-client-with-hazelcast-imdg)
  * [7.1. Python Client API Overview](#71-python-client-api-overview)
  * [7.2. Python Client Operation Modes](#72-python-client-operation-modes)
      * [7.2.1. Smart Client](#721-smart-client)
      * [7.2.2. Unisocket Client](#722-unisocket-client)
  * [7.3. Handling Failures](#73-handling-failures)
    * [7.3.1. Handling Client Connection Failure](#731-handling-client-connection-failure)
    * [7.3.2. Handling Retry-able Operation Failure](#732-handling-retry-able-operation-failure)
  * [7.4. Using Distributed Data Structures](#74-using-distributed-data-structures)
    * [7.4.1. Using Map](#741-using-map)
    * [7.4.2. Using MultiMap](#742-using-multimap)
    * [7.4.3. Using Replicated Map](#743-using-replicated-map)
    * [7.4.4. Using Queue](#744-using-queue)
    * [7.4.5. Using Set](#745-using-set)
    * [7.4.6. Using List](#746-using-list)
    * [7.4.7. Using Ringbuffer](#747-using-ringbuffer)
    * [7.4.8. Using Topic](#748-using-topic) 
    * [7.4.9. Using Lock](#749-using-lock)
    * [7.4.10. Using Atomic Long](#7410-using-atomic-long)
    * [7.4.11. Using Semaphore](#7411-using-semaphore)
    * [7.4.12. Using Transactions](#7412-using-transactions)
    * [7.4.13. Using PN Counter](#7413-using-pn-counter)
    * [7.4.14. Using Flake ID Generator](#7414-using-flake-id-generator)
  * [7.5. Distributed Events](#75-distributed-events)
    * [7.5.1. Cluster Events](#751-cluster-events)
      * [7.5.1.1. Listening for Member Events](#7511-listening-for-member-events)
      * [7.5.1.2. Listening for Lifecycle Events](#7512-listening-for-lifecycle-events)
    * [7.5.2. Distributed Data Structure Events](#752-distributed-data-structure-events)
      * [7.5.2.1. Listening for Map Events](#7521-listening-for-map-events)
  * [7.6. Distributed Computing](#76-distributed-computing)
    * [7.6.1. Using EntryProcessor](#761-using-entryprocessor)
  * [7.7. Distributed Query](#77-distributed-query)
    * [7.7.1. How Distributed Query Works](#771-how-distributed-query-works)
      * [7.7.1.1. Employee Map Query Example](#7711-employee-map-query-example)
      * [7.7.1.2. Querying by Combining Predicates with AND, OR, NOT](#7712-querying-by-combining-predicates-with-and-or-not)
      * [7.7.1.3. Querying with SQL](#7713-querying-with-sql)
      * [7.7.1.4. Querying with JSON Strings](#7714-querying-with-json-strings)
  * [7.8. Performance](#78-performance)
    * [7.8.1. Near Cache](#781-near-cache)
      * [7.8.1.1. Configuring Near Cache](#7811-configuring-near-cache)
      * [7.8.1.2. Near Cache Example for Map](#7812-near-cache-example-for-map)
      * [7.8.1.3. Near Cache Eviction](#7813-near-cache-eviction)
      * [7.8.1.4. Near Cache Expiration](#7814-near-cache-expiration)
      * [7.8.1.5. Near Cache Invalidation](#7815-near-cache-invalidation)
  * [7.9. Monitoring and Logging](#79-monitoring-and-logging)
    * [7.9.1. Enabling Client Statistics](#791-enabling-client-statistics)
    * [7.9.2. Logging Configuration](#792-logging-configuration)
* [8. Development and Testing](#8-development-and-testing)
  * [8.1. Building and Using Client From Sources](#81-building-and-using-client-from-sources)
  * [8.2. Testing](#82-testing)
* [9. Getting Help](#9-getting-help)
* [10. Contributing](#10-contributing)
* [11. License](#11-license)
* [12. Copyright](#12-copyright)

# Introduction

This document provides information about the Python client for [Hazelcast](https://hazelcast.org/). This client uses Hazelcast's [Open Client Protocol](https://hazelcast.org/documentation/#open-binary) and works with Hazelcast IMDG 3.6 and higher versions.

### Resources

See the following for more information on Python and Hazelcast IMDG:

* Hazelcast IMDG [website](https://hazelcast.org/)
* Hazelcast IMDG [Reference Manual](https://hazelcast.org/documentation/#imdg)
* About [Python](https://www.python.org/about/)

### Release Notes

See the [Releases](https://github.com/hazelcast/hazelcast-python-client/releases) page of this repository.

# 1. Getting Started

This chapter provides information on how to get started with your Hazelcast Python client. It outlines the requirements, installation and configuration of the client, setting up a cluster, and provides a simple application that uses a distributed map in Python client.

## 1.1. Requirements

- Windows, Linux/UNIX or Mac OS X
- Python 2.7 or Python 3.4 or newer
- Java 6 or newer
- Hazelcast IMDG 3.6 or newer
- Latest Hazelcast Python client

## 1.2. Working with Hazelcast IMDG Clusters

Hazelcast Python client requires a working Hazelcast IMDG cluster to run. This cluster handles storage and manipulation of the user data.
Clients are a way to connect to the Hazelcast IMDG cluster and access such data.

Hazelcast IMDG cluster consists of one or more cluster members. These members generally run on multiple virtual or physical machines
and are connected to each other via network. Any data put on the cluster is partitioned to multiple members transparent to the user.
It is therefore very easy to scale the system by adding new members as the data grows. Hazelcast IMDG cluster also offers resilience. Should
any hardware or software problem causes a crash to any member, the data on that member is recovered from backups and the cluster
continues to operate without any downtime. Hazelcast clients are an easy way to connect to a Hazelcast IMDG cluster and perform tasks on
distributed data structures that live on the cluster.

In order to use Hazelcast Python client, we first need to setup a Hazelcast IMDG cluster.

### 1.2.1. Setting Up a Hazelcast IMDG Cluster

There are following options to start a Hazelcast IMDG cluster easily:

* You can run standalone members by downloading and running JAR files from the website.
* You can embed members to your Java projects. 

We are going to download JARs from the website and run a standalone member for this guide.

#### 1.2.1.1. Running Standalone JARs

Follow the instructions below to create a Hazelcast IMDG cluster:

1. Go to Hazelcast's download [page](https://hazelcast.org/download/) and download either the `.zip` or `.tar` distribution of Hazelcast IMDG.
2. Decompress the contents into any directory that you
want to run members from.
3. Change into the directory that you decompressed the Hazelcast content and then into the `bin` directory.
4. Use either `start.sh` or `start.bat` depending on your operating system. Once you run the start script, you should see the Hazelcast IMDG logs in the terminal.

You should see a log similar to the following, which means that your 1-member cluster is ready to be used:

```
INFO: [192.168.0.3]:5701 [dev] [3.10.4]

Members {size:1, ver:1} [
	Member [192.168.0.3]:5701 - 65dac4d1-2559-44bb-ba2e-ca41c56eedd6 this
]

Sep 06, 2018 10:50:23 AM com.hazelcast.core.LifecycleService
INFO: [192.168.0.3]:5701 [dev] [3.10.4] [192.168.0.3]:5701 is STARTED
```

#### 1.2.1.2. Adding User Library to CLASSPATH

When you want to use features such as querying and language interoperability, you might need to add your own Java classes to the Hazelcast member in order to use them from your Python client. This can be done by adding your own compiled code to the `CLASSPATH`. To do this, compile your code with the `CLASSPATH` and add the compiled files to the `user-lib` directory in the extracted `hazelcast-<version>.zip` (or `tar`). Then, you can start your Hazelcast member by using the start scripts in the `bin` directory. The start scripts will automatically add your compiled classes to the `CLASSPATH`.

Note that if you are adding an `IdentifiedDataSerializable` or a `Portable` class, you need to add its factory too. Then, you should configure the factory in the `hazelcast.xml` configuration file. This file resides in the `bin` directory where you extracted the `hazelcast-<version>.zip` (or `tar`).

The following is an example configuration when you are adding an `IdentifiedDataSerializable` class:

 ```xml
<hazelcast>
     ...
     <serialization>
        <data-serializable-factories>
            <data-serializable-factory factory-id=<identified-factory-id>>
                IdentifiedFactoryClassName
            </data-serializable-factory>
        </data-serializable-factories>
    </serialization>
    ...
</hazelcast>
```

If you want to add a `Portable` class, you should use `<portable-factories>` instead of `<data-serializable-factories>` in the above configuration.

See the [Hazelcast IMDG Reference Manual](http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#getting-started) for more information on setting up the clusters.

## 1.3. Downloading and Installing

You can download and install the Python client from [PyPI](https://pypi.org/project/hazelcast-python-client/) using pip. Run the following command:

```
pip install hazelcast-python-client
```

Alternatively, it can be installed from the source using the following command:

```
python setup.py install
```

## 1.4. Basic Configuration

If you are using Hazelcast IMDG and Python client on the same computer, generally the default configuration should be fine. This is great for
trying out the client. However, if you run the client on a different computer than any of the cluster members, you may
need to do some simple configurations such as specifying the member addresses.

The Hazelcast IMDG members and clients have their own configuration options. You may need to reflect some of the member side configurations on the client side to properly connect to the cluster.

This section describes the most common configuration elements to get you started in no time.
It discusses some member side configuration options to ease the understanding of Hazelcast's ecosystem. Then, the client side configuration options
regarding the cluster connection are discussed. The configurations for the Hazelcast IMDG data structures that can be used in the Python client are discussed in the following sections.

See the [Hazelcast IMDG Reference Manual](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html) and [Configuration Overview section](#3-configuration-overview) for more information.

### 1.4.1. Configuring Hazelcast IMDG

Hazelcast IMDG aims to run out-of-the-box for most common scenarios. However if you have limitations on your network such as multicast being disabled,
you may have to configure your Hazelcast IMDG members so that they can find each other on the network. Also, since most of the distributed data structures are configurable, you may want to configure them according to your needs. We will show you the basics about network configuration here.

You can use the following options to configure Hazelcast IMDG:

* Using the `hazelcast.xml` configuration file.
* Programmatically configuring the member before starting it from the Java code.

Since we use standalone servers, we will use the `hazelcast.xml` file to configure our cluster members.

When you download and unzip `hazelcast-<version>.zip` (or `tar`), you see the `hazelcast.xml` in the `bin` directory. When a Hazelcast member starts, it looks for the `hazelcast.xml` file to load the configuration from. A sample `hazelcast.xml` is shown below.

```xml
<hazelcast>
    <group>
        <name>dev</name>
        <password>dev-pass</password>
    </group>
    <network>
        <port auto-increment="true" port-count="100">5701</port>
        <join>
            <multicast enabled="true">
                <multicast-group>224.2.2.3</multicast-group>
                <multicast-port>54327</multicast-port>
            </multicast>
            <tcp-ip enabled="false">
                <interface>127.0.0.1</interface>
                <member-list>
                    <member>127.0.0.1</member>
                </member-list>
            </tcp-ip>
        </join>
        <ssl enabled="false"/>
    </network>
    <partition-group enabled="false"/>
    <map name="default">
        <backup-count>1</backup-count>
    </map>
</hazelcast>
```

We will go over some important configuration elements in the rest of this section.

- `<group>`: Specifies which cluster this member belongs to. A member connects only to the other members that are in the same group as
itself. As shown in the above configuration sample, there are `<name>` and `<password>` tags under the `<group>` element with some pre-configured values. You may give your clusters different names so that they can
live in the same network without disturbing each other. Note that the cluster name should be the same across all members and clients that belong
 to the same cluster. The `<password>` tag is not in use since Hazelcast 3.9. It is there for backward compatibility
purposes. You can remove or leave it as it is if you use Hazelcast 3.9 or later.
- `<network>`
    - `<port>`: Specifies the port number to be used by the member when it starts. Its default value is 5701. You can specify another port number, and if
     you set `auto-increment` to `true`, then Hazelcast will try the subsequent ports until it finds an available port or the `port-count` is reached.
    - `<join>`: Specifies the strategies to be used by the member to find other cluster members. Choose which strategy you want to
    use by setting its `enabled` attribute to `true` and the others to `false`.
        - `<multicast>`: Members find each other by sending multicast requests to the specified address and port. It is very useful if IP addresses
        of the members are not static.
        - `<tcp>`: This strategy uses a pre-configured list of known members to find an already existing cluster. It is enough for a member to
        find only one cluster member to connect to the cluster. The rest of the member list is automatically retrieved from that member. We recommend
        putting multiple known member addresses there to avoid disconnectivity should one of the members in the list is unavailable at the time
        of connection.

These configuration elements are enough for most connection scenarios. Now we will move onto the configuration of the Python client.

### 1.4.2. Configuring Hazelcast Python Client

Hazelcast Python client can be configured programmatically.

This section describes some network configuration settings to cover common use cases in connecting the client to a cluster. See the [Configuration Overview section](#3-configuration-overview)
and the following sections for information about detailed network configurations and/or additional features of Hazelcast Python client configuration.

An easy way to configure your Hazelcast Python client is to create a `ClientConfig` object and set the appropriate options. Then you can
supply this object to your client at the startup. This is the programmatic configuration approach.

Once you imported `hazelcast` to your Python project, you may follow programmatic configuration approach.

You need to create a `ClientConfig` object and adjust its properties. Then you can pass this object to the client when starting it.

```python
import hazelcast

config = hazelcast.ClientConfig()
client = hazelcast.HazelcastClient(config)
```

---

If you run the Hazelcast IMDG members in a different server than the client, you most probably have configured the members' ports and cluster
names as explained in the previous section. If you did, then you need to make certain changes to the network settings of your client.


#### 1.4.2.1. Group Settings

You need to provide the group name of the cluster, if it is defined on the server side, to which you want the client to connect.

```python
config = hazelcast.ClientConfig()
config.group_config.name = "group-name-of-your-cluster"
config.group_config.password = "group password"
```

> **NOTE: If you have a Hazelcast IMDG release older than 3.11, you need to provide also a group password along with the group name.**

#### 1.4.2.2. Network Settings

You need to provide the IP address and port of at least one member in your cluster so the client can find it.

```python
import hazelcast

config = hazelcast.ClientConfig()
config.network_config.addresses.append("IP-address:port")
```

### 1.4.3. Client System Properties

While configuring your Python client, you can use various system properties provided by Hazelcast to tune its clients. These properties can be set programmatically through `config.set_property` method or by using an environment variable.

The value of the any property will be:

* the programmatically configured value, if programmatically set,
* the environment variable value, if the environment variable is set,
* the default value, if none of the above is set.

See the following for an example client system property configuration:
 
**Programmatically:**

 ```python
from hazelcast.config import ClientProperties

# Sets invocation timeout as 2 seconds
config.set_property(ClientProperties.INVOCATION_TIMEOUT_SECONDS.name, 2) 
```

or 
 
 ```python
# Sets invocation timeout as 2 seconds
config.set_property("hazelcast.client.invocation.timeout.seconds", 2)
```

 **By using an environment variable:** 

```python
import os

environ = os.environ
environ[ClientProperties.INVOCATION_TIMEOUT_SECONDS.name] = "2"
```

If you set a property both programmatically and via an environment variable, the programmatically set value will be used.

See the [complete list](http://hazelcast.github.io/hazelcast-python-client/3.10/hazelcast.config.html#hazelcast.config.ClientProperties) of client system properties, along with their descriptions, which can be used to configure your Hazelcast Python client.

## 1.5. Basic Usage

Now that we have a working cluster and we know how to configure both our cluster and client, we can run a simple program to use a
distributed map in the Python client.

The following example first configures the logger for the Python client. You can find more information about the logging options in the [Logging Configuration section](#792-logging-configuration). 

Then, it creates a configuration object and starts a client.

```python
import hazelcast

# We create a config for illustrative purposes.
# We do not adjust this config. Therefore it has default settings.
config = hazelcast.ClientConfig() 

# Client connects to the cluster with the given configuration. 
client = hazelcast.HazelcastClient(config) 
```

This should print logs about the cluster members such as address, port and UUID to the `stderr`.

```
Feb 15, 2019 12:51:59 PM HazelcastClient
INFO: [3.10] [dev] [hz.client_0] A non-empty group password is configured for the Hazelcast client. Starting with Hazelcast IMDG version 3.11, clients with the same group name, but with different group passwords (that do not use authentication) will be accepted to a cluster. The group password configuration will be removed completely in a future release.
Feb 15, 2019 12:51:59 PM HazelcastClient.LifecycleService
INFO: [3.10] [dev] [hz.client_0] (20181119 - 9080a46) HazelcastClient is STARTING
Feb 15, 2019 12:51:59 PM HazelcastClient.ClusterService
INFO: [3.10] [dev] [hz.client_0] Connecting to Address(host=127.0.0.1, port=5701)
Feb 15, 2019 12:51:59 PM HazelcastClient.ConnectionManager
INFO: [3.10] [dev] [hz.client_0] Authenticated with Connection(address=('127.0.0.1', 5701), id=0)
Feb 15, 2019 12:51:59 PM HazelcastClient.ClusterService
INFO: [3.10] [dev] [hz.client_0] New member list:

Members [1] {
	Member [10.216.1.49]:5701 - 1f4bb35d-b68f-46eb-bd65-61e3f4bc9922
}

Feb 15, 2019 12:51:59 PM HazelcastClient.LifecycleService
INFO: [3.10] [dev] [hz.client_0] (20181119 - 9080a46) HazelcastClient is CONNECTED
Feb 15, 2019 12:51:59 PM HazelcastClient
INFO: [3.10] [dev] [hz.client_0] Client started.
```

Congratulations. You just started a Hazelcast Python client.

**Using a Map**

Let's manipulate a distributed map on a cluster using the client.

```python
import hazelcast

config = hazelcast.ClientConfig()
client = hazelcast.HazelcastClient(config)

personnel_map = client.get_map("personnel-map")
personnel_map.put("Alice", "IT")
personnel_map.put("Bob", "IT")
personnel_map.put("Clark", "IT")

print("Added IT personnel. Printing all known personnel")

for person, department in personnel_map.entry_set().result():
    print("{} is in {} department".format(person, department))
    
client.shutdown()
```

**Output**

```
Feb 15, 2019 12:53:15 PM HazelcastClient
INFO: [3.10] [dev] [hz.client_0] A non-empty group password is configured for the Hazelcast client. Starting with Hazelcast IMDG version 3.11, clients with the same group name, but with different group passwords (that do not use authentication) will be accepted to a cluster. The group password configuration will be removed completely in a future release.
Feb 15, 2019 12:53:15 PM HazelcastClient.LifecycleService
INFO: [3.10] [dev] [hz.client_0] (20181119 - 9080a46) HazelcastClient is STARTING
Feb 15, 2019 12:53:15 PM HazelcastClient.ClusterService
INFO: [3.10] [dev] [hz.client_0] Connecting to Address(host=127.0.0.1, port=5701)
Feb 15, 2019 12:53:15 PM HazelcastClient.ConnectionManager
INFO: [3.10] [dev] [hz.client_0] Authenticated with Connection(address=('127.0.0.1', 5701), id=0)
Feb 15, 2019 12:53:15 PM HazelcastClient.ClusterService
INFO: [3.10] [dev] [hz.client_0] New member list:

Members [1] {
	Member [10.216.1.49]:5701 - 1f4bb35d-b68f-46eb-bd65-61e3f4bc9922
}

Feb 15, 2019 12:53:15 PM HazelcastClient.LifecycleService
INFO: [3.10] [dev] [hz.client_0] (20181119 - 9080a46) HazelcastClient is CONNECTED
Feb 15, 2019 12:53:15 PM HazelcastClient
INFO: [3.10] [dev] [hz.client_0] Client started.
Added IT personnel. Printing all known personnel
Alice is in IT department
Clark is in IT department
Bob is in IT department
Feb 15, 2019 12:53:15 PM HazelcastClient.LifecycleService
INFO: [3.10] [dev] [hz.client_0] (20181119 - 9080a46) HazelcastClient is SHUTTING_DOWN
Feb 15, 2019 12:53:15 PM HazelcastClient.AsyncoreReactor
WARNING: [3.10] [dev] [hz.client_0] Connection closed by server
Feb 15, 2019 12:53:15 PM HazelcastClient.LifecycleService
INFO: [3.10] [dev] [hz.client_0] (20181119 - 9080a46) HazelcastClient is SHUTDOWN
Feb 15, 2019 12:53:15 PM HazelcastClient
INFO: [3.10] [dev] [hz.client_0] Client shutdown.
```

You see this example puts all the IT personnel into a cluster-wide `personnel-map` and then prints all the known personnel.

Now, run the following code.

```python
personnel_map = client.get_map("personnel-map")
personnel_map.put("Denise", "Sales")
personnel_map.put("Erwing", "Sales")
personnel_map.put("Faith", "Sales")

print("Added Sales personnel. Printing all known personnel")

for person, department in personnel_map.entry_set().result():
    print("{} is in {} department".format(person, department))
```

**Output**

```
Feb 15, 2019 12:54:05 PM HazelcastClient
INFO: [3.10] [dev] [hz.client_0] A non-empty group password is configured for the Hazelcast client. Starting with Hazelcast IMDG version 3.11, clients with the same group name, but with different group passwords (that do not use authentication) will be accepted to a cluster. The group password configuration will be removed completely in a future release.
Feb 15, 2019 12:54:05 PM HazelcastClient.LifecycleService
INFO: [3.10] [dev] [hz.client_0] (20181119 - 9080a46) HazelcastClient is STARTING
Feb 15, 2019 12:54:05 PM HazelcastClient.ClusterService
INFO: [3.10] [dev] [hz.client_0] Connecting to Address(host=127.0.0.1, port=5701)
Feb 15, 2019 12:54:05 PM HazelcastClient.ConnectionManager
INFO: [3.10] [dev] [hz.client_0] Authenticated with Connection(address=('127.0.0.1', 5701), id=0)
Feb 15, 2019 12:54:05 PM HazelcastClient.ClusterService
INFO: [3.10] [dev] [hz.client_0] New member list:

Members [1] {
	Member [10.216.1.49]:5701 - 1f4bb35d-b68f-46eb-bd65-61e3f4bc9922
}

Feb 15, 2019 12:54:05 PM HazelcastClient.LifecycleService
INFO: [3.10] [dev] [hz.client_0] (20181119 - 9080a46) HazelcastClient is CONNECTED
Feb 15, 2019 12:54:05 PM HazelcastClient
INFO: [3.10] [dev] [hz.client_0] Client started.
Added Sales personnel. Printing all known personnel
Denise is in Sales department
Erwing is in Sales department
Faith is in Sales department
Alice is in IT department
Clark is in IT department
Bob is in IT department
Feb 15, 2019 12:54:05 PM HazelcastClient.LifecycleService
INFO: [3.10] [dev] [hz.client_0] (20181119 - 9080a46) HazelcastClient is SHUTTING_DOWN
Feb 15, 2019 12:54:05 PM HazelcastClient.AsyncoreReactor
WARNING: [3.10] [dev] [hz.client_0] Connection closed by server
Feb 15, 2019 12:54:05 PM HazelcastClient.LifecycleService
INFO: [3.10] [dev] [hz.client_0] (20181119 - 9080a46) HazelcastClient is SHUTDOWN
Feb 15, 2019 12:54:05 PM HazelcastClient
INFO: [3.10] [dev] [hz.client_0] Client shutdown.
```

You will see this time we add only the sales employees but we get the list of all known employees including the ones in IT.
That is because our map lives in the cluster and no matter which client we use, we can access the whole map.

You may wonder why we have used `result()` method over the `entry_set()` method of the `personnel_map`. That is because 
the Hazelcast Python client is designed to be fully asynchronous. Every method call over distributed objects such as 
`put()`, `get()`, `entry_set()`, etc. will return a `Future` object that is similar to the [Future](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Future)
class of the [concurrent.futures](https://docs.python.org/3/library/concurrent.futures.html#module-concurrent.futures) module.

With this design choice, method calls over the distributed objects can be executed asynchronously without blocking the 
execution order of your program. 

You may get the value returned by the method calls using the `result()` method of the `Future` class.
`result` will block the execution of your program and will wait until the future finishes running. Then, it will return the
value returned by the call which are key-value pairs in our `entry_set()` method call. 

You may also attach a function to the future objects that will be called, with the future as its only argument, when the future finishes running.

For example, the part where we printed the personnel in above code can be rewritten with a callback attached to the `entry_set()`, as shown below..

```python
import time

def entry_set_cb(future):
    for person, department in future.result():
        print("{} is in {} department".format(person, department))

personnel_map.entry_set().add_done_callback(entry_set_cb)
time.sleep(0.1) # wait for Future to complete
```

Asynchronous operations are far more efficient in single threaded Python interpreter but you may want all of your method calls
over distributed objects to be blocking. For this purpose, Hazelcast Python client provides a helper method called `blocking()`. 
This method blocks the execution of your program for all the method calls over distributed objects
until the return value of your call is calculated and returns that value directly instead of a `Future` object.

To make the `personnel_map` presented previously in this section blocking, you need to call `blocking()` method over it.

```python
personnel_map = client.get_map("personnel-map").blocking()
``` 

Now, all the methods over the `personnel_map`, such as `put()` and `entry_set()`,  will be blocking. So, you don't need to call `result()`
over it or attach a callback to it anymore.

```python
for person, department in personnel_map.entry_set():
    print("{} is in {} department".format(person, department))
```  

## 1.6. Code Samples

See the Hazelcast Python [examples](https://github.com/hazelcast/hazelcast-python-client/tree/master/examples) for more code samples.

You can also see the [latest Hazelcast Python API Documentation](http://hazelcast.github.io/hazelcast-python-client/3.10/index.html) or [global API Documentation page](http://hazelcast.github.io/hazelcast-python-client/).

# 2. Features

Hazelcast Python client supports the following data structures and features:

* Map
* Queue
* Set
* List
* MultiMap
* Replicated Map
* Ringbuffer
* Topic
* Lock
* Semaphore
* AtomicLong
* CRDT PN Counter
* AtomicReference
* IdGenerator
* CountDownLatch
* Distributed Executor Service
* Event Listeners
* Sub-Listener Interfaces for Map Listener
* Entry Processor
* Transactional Map
* Transactional MultiMap
* Transactional Queue
* Transactional List
* Transactional Set
* Query (Predicates)
* Entry Processor
* Built-in Predicates
* Listener with Predicate
* Near Cache Support
* Programmatic Configuration
* SSL Support (requires Enterprise server)
* Mutual Authentication (requires Enterprise server)
* Authorization
* Smart Client
* Unisocket Client
* Lifecycle Service
* Hazelcast Cloud Discovery
* IdentifiedDataSerializable Serialization
* Portable Serialization
* Custom Serialization
* JSON Serialization
* Global Serialization

# 3. Configuration Overview

This chapter describes the options to configure your Python client.

## 3.1. Configuration Options

You can configure Hazelcast Python client programmatically (API).

### 3.1.1. Programmatic Configuration

For programmatic configuration of the Hazelcast Python client, just instantiate a `ClientConfig` object and configure the
desired aspects. An example is shown below.

```python
config = hazelcast.ClientConfig()
config.network_config.addresses.append("127.0.0.1:5701")
client = hazelcast.HazelcastClient(config)
```

See the `ClientConfig` class documentation at [Hazelcast Python Client API Docs](http://hazelcast.github.io/hazelcast-python-client/3.10/hazelcast.config.html) for details.

# 4. Serialization

Serialization is the process of converting an object into a stream of bytes to store the object in the memory, a file or database, or transmit it through the network. Its main purpose is to save the state of an object in order to be able to recreate it when needed. The reverse process is called deserialization. Hazelcast offers you its own native serialization methods. You will see these methods throughout this chapter.

Hazelcast serializes all your objects before sending them to the server. The `bool`, `int`, `long` (for Python 2), `float`, `str`, and `unicode` (for Python 2)  types are serialized natively and you cannot override this behavior. The following table is the conversion of types for the Java server side.

| Python  | Java                                   |
|---------|----------------------------------------|
| bool    | Boolean                                |
| int     | Byte, Short, Integer, Long, BigInteger |
| long    | Byte, Short, Integer, Long, BigInteger |
| float   | Float, Double                          |
| str     | String                                 |
| unicode | String                                 |

> Note: A `int` or `long` type is serialized as `Integer` by default. You can configure this behavior using the `SerializationConfig.default_integer_type`.

Arrays of the above types can be serialized as `boolean[]`, `byte[]`, `short[]`, `int[]`, `float[]`, `double[]`, `long[]` and `string[]` for the Java server side, respectively. 

**Serialization Priority**

When Hazelcast Python client serializes an object:

1. It first checks whether the object is `None`.

2. If the above check fails, then it checks if it is an instance of `IdentifiedDataSerializable`.

3. If the above check fails, then it checks if it is an instance of `Portable`.

4. If the above check fails, then it checks if it is an instance of one of the default types (see the default types above).

5. If the above check fails, then it looks for a user-specified [Custom Serialization](#43-custom-serialization).

6. If the above check fails, it will use the registered [Global Serialization](#45-global-serialization) if one exists.

7. If the above check fails, then the Python client uses `cPickle` (for Python 2) or `pickle` (for Python 3) by default.

However, `cPickle/pickle Serialization` is not the best way of serialization in terms of performance and interoperability between the clients in different languages. If you want the serialization to work faster or you use the clients in different languages, Hazelcast offers its own native serialization types, such as [`IdentifiedDataSerializable` Serialization](#41-identifieddataserializable-serialization) and [`Portable` Serialization](#42-portable-serialization).

On top of all, if you want to use your own serialization type, you can use a [Custom Serialization](#43-custom-serialization).

## 4.1. IdentifiedDataSerializable Serialization

For a faster serialization of objects, Hazelcast recommends to extend the `IdentifiedDataSerializable` class.

The following is an example of a class that extends `IdentifiedDataSerializable`:

```python
from hazelcast.serialization.api import IdentifiedDataSerializable

class Address(IdentifiedDataSerializable):
    def __init__(self, street=None, zip_code=None, city=None, state=None):
        self.street = street
        self.zip_code = zip_code
        self.city = city
        self.state = state
        
    def get_class_id(self):
        return 1
    
    def get_factory_id(self):
        return 1
    
    def write_data(self, object_data_output):
        object_data_output.write_utf(self.street)
        object_data_output.write_int(self.zip_code)
        object_data_output.write_utf(self.city)
        object_data_output.write_utf(self.state)
        
    def read_data(self, object_data_input):
        self.street = object_data_input.read_utf()
        self.zip_code = object_data_input.read_int()
        self.city = object_data_input.read_utf()
        self.state = object_data_input.read_utf()
```
> Note: For IdentifiedDataSerializable to work in Python client, the class that inherits it should have default valued parameters in its `__init__` method so that an instance of that class can be created without passing any arguments to it.  

The IdentifiedDataSerializable uses `get_class_id()` and `get_factory_id()` methods to reconstitute the object. To complete the implementation, an `IdentifiedDataSerializable factory` should also be created and registered into the `SerializationConfig` which can be accessed from `Config.serialization_config`. A factory is a dictionary that stores class ID and the `IdentifiedDataSerializable` class type pairs as the key and value. The factory's responsibility is to store the right `IdentifiedDataSerializable` class type for the given class ID. 

A sample `IdentifiedDataSerializable factory` could be created as follows:

```python
factory = {1: Address}
```

Note that the keys of the dictionary should be the same as the class IDs of their corresponding `IdentifiedDataSerializable` class types.

The last step is to register the `IdentifiedDataSerializable factory` to the `SerializationConfig`.

```python
config.serialization_config.data_serializable_factories[1] = factory
```

Note that the ID that is passed to the `SerializationConfig` is same as the factory ID that the `Address` class returns.

## 4.2. Portable Serialization

As an alternative to the existing serialization methods, Hazelcast offers portable serialization. To use it, you need to extend the `Portable` class. Portable serialization has the following advantages:

- Supporting multiversion of the same object type.
- Fetching individual fields without having to rely on the reflection.
- Querying and indexing support without deserialization and/or reflection.

In order to support these features, a serialized `Portable` object contains meta information like the version and concrete location of the each field in the binary data. This way Hazelcast is able to navigate in the binary data and deserialize only the required field without actually deserializing the whole object which improves the query performance.

With multiversion support, you can have two members where each of them having different versions of the same object, and Hazelcast will store both meta information and use the correct one to serialize and deserialize portable objects depending on the member. This is very helpful when you are doing a rolling upgrade without shutting down the cluster.

Also note that portable serialization is totally language independent and is used as the binary protocol between Hazelcast server and clients.

A sample portable implementation of a `Foo` class looks like the following:

```python
from hazelcast.serialization.api import Portable

class Foo(Portable):
    def __init__(self, foo=None):
        self.foo = foo
        
    def get_class_id(self):
        return 1
        
    def get_factory_id(self):
        return 1
        
    def write_portable(self, writer):
        writer.write_utf("foo", self.foo)
    
    def read_portable(self, reader):
        self.foo = reader.read_utf("foo")
```

> Note: For Portable to work in Python client, the class that inherits it should have default valued parameters in its `__init__` method so that an instance of that class can be created without passing any arguments to it.  

Similar to `IdentifiedDataSerializable`, a `Portable` class must provide the `get_class_id()` and `get_factory_id()` methods. The factory dictionary will be used to create the `Portable` object given the class ID.

A sample `Portable factory` could be created as follows:

```python
factory = {1: Foo}
```

Note that the keys of the dictionary should be the same as the class IDs of their corresponding `Portable` class types.

The last step is to register the `Portable factory` to the `SerializationConfig`.

```python
config.serialization_config.data_serializable_factories[1] = factory
```

Note that the ID that is passed to the `SerializationConfig` is same as the factory ID that `Foo` class returns.

## 4.3. Custom Serialization

Hazelcast lets you plug a custom serializer to be used for serialization of objects.

Let's say you have a class called `Musician` and you would like to customize the serialization for it, since you may want to use an external serializer for only one class.

```python
class Musician:
    def __init__(self, name):
        self.name = name
```

Let's say your custom `MusicianSerializer` will serialize `Musician`. This time, your custom serializer must extend the `StreamSerializer` class.

```python
from hazelcast.serialization.api import StreamSerializer

class MusicianSerializer(StreamSerializer):
    def get_type_id(self):
        return 10
    
    def destroy(self):
        pass
    
    def write(self, out, obj):
        out.write_int(len(obj.name))
        for s in obj.name:
            out.write_char(s)
    
    def read(self, inp):
        length = inp.read_int()
        name = ""
        for i in range(length):
            name += chr(inp.read_int())
        return Musician(name)
```

Note that the serializer `id` must be unique as Hazelcast will use it to lookup the `MusicianSerializer` while it deserializes the object. Now the last required step is to register the `MusicianSerializer` to the configuration.


```python
config.serialization_config.set_custom_serializer(Musician, MusicianSerializer)
```

From now on, Hazelcast will use `MusicianSerializer` to serialize `Musician` objects.

## 4.4. JSON Serialization

You can use the JSON formatted strings as objects in Hazelcast cluster. Starting with Hazelcast IMDG 3.12, the JSON serialization is one of the formerly supported serialization methods. Creating JSON objects in the cluster does not require any server side coding and hence you can just send a JSON formatted string object to the cluster and query these objects by fields.

In order to use JSON serialization, you should use the `HazelcastJsonValue` object for the key or value. Here is an example IMap usage:

`HazelcastJsonValue` is a simple wrapper and identifier for the JSON formatted strings. You can get the JSON string from the `HazelcastJsonValue` object using the `to_string()` method. 

You can construct `HazelcastJsonValue` from strings or JSON serializable Python objects. If a Python object is provided to the constructor, `HazelcastJsonValue` tries to convert it
to a JSON string. If an error occurs during the conversion, it is raised directly. If a string argument is provided to the constructor, it is used as it is. 

No JSON parsing is performed but it is your responsibility to provide correctly formatted JSON strings. The client will not validate the string, and it will send it to the cluster as it is. If you submit incorrectly formatted JSON strings and, later, if you query those objects, it is highly possible that you will get formatting errors since the server will fail to deserialize or find the query fields.

Here is an example of how you can construct a `HazelcastJsonValue` and put to the map:

```python
# From JSON string
json_map.put("item1", HazelcastJsonValue("{\"age\": 4}"))

# # From JSON serializable object
json_map.put("item2", HazelcastJsonValue({"age": 20}))
```

You can query JSON objects in the cluster using the `Predicate`s of your choice. An example JSON query for querying the values whose age is less than 6 is shown below:

```python
# Get the objects whose age is less than 6
result = json_map.values(is_less_than_or_equal_to("age", 6))
print("Retrieved {} values whose age is less than 6.".format(len(result)))
print("Entry is {}".format(result[0].to_string()))
```

## 4.5. Global Serialization

The global serializer is identical to custom serializers from the implementation perspective. The global serializer is registered as a fallback serializer to handle all other objects if a serializer cannot be located for them.

By default, `cPickle/pickle` serialization is used if the class is not `IdentifiedDataSerializable` or `Portable` or there is no custom serializer for it. When you configure a global serializer, it is used instead of `cPickle/pickle` serialization.

**Use Cases:**

* Third party serialization frameworks can be integrated using the global serializer.

* For your custom objects, you can implement a single serializer to handle all of them.

A sample global serializer that integrates with a third party serializer is shown below.

```python
import some_third_party_serializer
from hazelcast.serialization.api import StreamSerializer

class GlobalSerializer(StreamSerializer):
    def get_type_id(self):
        return 20
    
    def destroy(self):
        pass
    
    def write(self, out, obj):
        out.write_utf(some_third_party_serializer.serialize(obj))
    
    def read(self, inp):
        return some_third_party_serializer.deserialize(inp.read_utf())
```

You should register the global serializer in the configuration.


```python
config.serialization_config.global_serializer = GlobalSerializer
```

# 5. Setting Up Client Network

All network related configuration of Hazelcast Python client is performed via the `ClientNetworkConfig` class when using programmatic configuration. Let's first give the examples for this approach. Then we will look at its sub-elements and attributes.

Here is an example of configuring the network for Python Client programmatically.

```python
config.network_config.addresses.extend(["10.1.1.21""10.1.1.22:5703"])
config.network_config.smart_routing = True
config.network_config.redo_operation = True
config.network_config.connection_timeout = 6.0
config.network_config.connection_attempt_period = 5.0
config.network_config.connection_attempt_limit = 5
```

## 5.1. Providing Member Addresses

Address list is the initial list of cluster addresses which the client will connect to. The client uses this
list to find an alive member. Although it may be enough to give only one address of a member in the cluster
(since all members communicate with each other), it is recommended that you give the addresses for all the members.

```python
config.network_config.addresses.append("10.1.1.21") # single value
config.network_config.addresses.extend(["10.1.1.23", "10.1.1.22:5703"]) # multiple values
```

If the port part is omitted, then 5701, 5702 and 5703 will be tried in a random order.

You can specify multiple addresses with or without the port information as seen above. The provided list is shuffled and tried in a random order. Its default value is `localhost`.

## 5.2. Setting Smart Routing

Smart routing defines whether the client mode is smart or unisocket. See the [Python Client Operation Modes section](#72-python-client-operation-modes)
for the description of smart and unisocket modes.
 
The following is an example configuration.

```python
config.network_config.smart_routing = True
```

Its default value is `True` (smart client mode).

## 5.3. Enabling Redo Operation

It enables/disables redo-able operations. While sending the requests to the related members, the operations can fail due to various reasons. Read-only operations are retried by default. If you want to enable retry for the other operations, you can set the `redo_operation` to `True`.

```python
config.network_config.redo_operation = True
```

Its default value is `False` (disabled).

## 5.4. Setting Connection Timeout

Connection timeout is the timeout value in seconds for the members to accept the client connection requests.
If the member does not respond within the timeout, the client will retry to connect as many as `ClientNetworkConfig.connection_attempt_limit` times.
 
The following is an example configuration.

```python
config.network_config.connection_timeout = 6.0
```

Its default value is `5.0` seconds.

## 5.5. Setting Connection Attempt Limit

While the client is trying to connect initially to one of the members in the `ClientNetworkConfig.addresses`, that member might not be available at that moment. Instead of giving up, throwing an error and stopping the client, the client will retry as many as `ClientNetworkConfig.connection_attempt_limit` times. This is also the case when the previously established connection between the client and that member goes down.

The following is an example configuration.

```python
config.network_config.connection_attempt_limit = 5
```

Its default value is `2`.

## 5.6. Setting Connection Attempt Period

Connection attempt period is the duration in seconds between the connection attempts defined by `ClientNetworkConfig.connection_attempt_limit`.
 
The following is an example configuration.

```python
config.network_config.connection_attempt_period = 5.0
```

Its default value is `3.0` seconds.

## 5.7. Enabling Client TLS/SSL

You can use TLS/SSL to secure the connection between the clients and members. If you want to enable TLS/SSL
for the client-cluster connection, you should set the SSL configuration. Please see the [TLS/SSL section](#61-tlsssl).

As explained in the [TLS/SSL section](#61-tlsssl), Hazelcast members have key stores used to identify themselves (to other members) and Hazelcast Python clients have certificate authorities used to define which members they can trust. Hazelcast has the mutual authentication feature which allows the Python clients also to have their private keys and public certificates, and members to have their certificate authorities so that the members can know which clients they can trust. See the [Mutual Authentication section](#613-mutual-authentication).

## 5.8. Enabling Hazelcast Cloud Discovery

The purpose of Hazelcast Cloud Discovery is to provide the clients the means to use IP addresses provided by `hazelcast orchestrator`. To enable Hazelcast Cloud Discovery, specify a token for the `discovery_token` field and set the `enabled` field to `True`.
 
The following is the example configuration.

```python
config.group_config.name = "hazel"
config.group_config.password = "cast"

config.network_config.ssl_config.enabled = True

config.network_config.cloud_config.enabled = True
config.network_config.cloud_config.discovery_token = "dc9220bc5d9"
```

To be able to connect to the provided IP addresses, you should use secure TLS/SSL connection between the client and members. Therefore, you should enable the SSL configuration as described in the [TLS/SSL for Hazelcast Python Client section](#612-tlsssl-for-hazelcast-python-clients).

# 6. Securing Client Connection

This chapter describes the security features of Hazelcast Python client. These include using TLS/SSL for connections between members and between clients and members, and mutual authentication. These security features require **Hazelcast IMDG Enterprise** edition.

### 6.1. TLS/SSL

One of the offers of Hazelcast is the TLS/SSL protocol which you can use to establish an encrypted communication across your cluster with key stores and trust stores.

* A Java `keyStore` is a file that includes a private key and a public certificate. The equivalent of a key store is the combination of `keyfile` and `certfile` at the Python client side.

* A Java `trustStore` is a file that includes a list of certificates trusted by your application which is named certificate authority. The equivalent of a trust store is a `cafile` at the Python client side.

You should set `keyStore` and `trustStore` before starting the members. See the next section on how to set `keyStore` and `trustStore` on the server side.

#### 6.1.1. TLS/SSL for Hazelcast Members

Hazelcast allows you to encrypt socket level communication between Hazelcast members and between Hazelcast clients and members, for end to end encryption. To use it, see the [TLS/SSL for Hazelcast Members section](http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#tls-ssl-for-hazelcast-members).

#### 6.1.2. TLS/SSL for Hazelcast Python Clients

TLS/SSL for the Hazelcast Python client can be configured using the `SSLConfig` class. Let's first give an example of a sample configuration and then go over the configuration options one by one:

```python
import hazelcast
from hazelcast.config import PROTOCOL

config = hazelcast.ClientConfig()
config.network_config.ssl_config.enabled = True
config.network_config.ssl_config.cafile = "/home/hazelcast/cafile.pem"
config.network_config.ssl_config.certfile = "/home/hazelcast/certfile.pem"
config.network_config.ssl_config.keyfile = "/home/hazelcast/keyfile.pem"
config.network_config.ssl_config.password = "hazelcast"
config.network_config.ssl_config.protocol = PROTOCOL.TLSv1_3
config.network_config.ssl_config.ciphers = "DHE-RSA-AES128-SHA:DHE-RSA-AES256-SHA"
```

##### Enabling TLS/SSL

TLS/SSL for the Hazelcast Python client can be enabled/disabled using the `enabled` option. When this option is set to `True`, TLS/SSL will be configured with respect to the other `SSLConfig` options. 
Setting this option to `False` will result in discarding the other `SSLConfig` options.

The following is an example configuration:

```python
config.network_config.ssl_config.enabled = True
```

Default value is `False` (disabled). 

##### Setting CA File

Certificates of the Hazelcast members can be validated against `cafile`. This option should point to the absolute path of the concatenated CA certificates in PEM format. 
When SSL is enabled and `cafile` is not set, a set of default CA certificates from default locations will be used.

The following is an example configuration:

```python
config.network_config.ssl_config.cafile = "/home/hazelcast/cafile.pem"
```

##### Setting Client Certificate

When mutual authentication is enabled on the member side, clients or other members should also provide a certificate file that identifies themselves.
Then, Hazelcast members can use these certificates to validate the identity of their peers. 

Client certificate can be set using the `certfile`. This option should point to the absolute path of the client certificate in PEM format.

The following is an example configuration:

```python
config.network_config.ssl_config.certfile = "/home/hazelcast/certfile.pem"
```

##### Setting Private Key

Private key of the `certfile` can be set using the `keyfile`. This option should point to the absolute path of the private key file for the client certificate in the PEM format.

If this option is not set, private key will be taken from `certfile`. In this case, `certfile` should be in the following format.

```
-----BEGIN RSA PRIVATE KEY-----
... (private key in base64 encoding) ...
-----END RSA PRIVATE KEY-----
-----BEGIN CERTIFICATE-----
... (certificate in base64 PEM encoding) ...
-----END CERTIFICATE-----
```

The following is an example configuration:

```python
config.network_config.ssl_config.keyfile = "/home/hazelcast/keyfile.pem"
```

##### Setting Password of the Private Key

If the private key is encrypted using a password, `password` will be used to decrypt it. The `password` may be a function to call to get the password.
In that case, it will be called with no arguments, and it should return a string, bytes or bytearray. If the return value is a string it will be encoded as UTF-8 before using it to decrypt the key.
        
Alternatively a string, bytes or bytearray value may be supplied directly as the password.

The following is an example configuration:

```python
config.network_config.ssl_config.password = "hazelcast"
```

##### Setting the Protocol

`protocol` can be used to select the protocol that will be used in the TLS/SSL communication. Hazelcast Python client offers the following protocols:

* **SSLv2**     : SSL 2.0 Protocol. *RFC 6176 prohibits the usage of SSL 2.0.* 
* **SSLv3**     : SSL 3.0 Protocol. *RFC 7568 prohibits the usage of SSL 3.0.*
* **SSL**       : Alias for SSL 3.0
* **TLSv1**     : TLS 1.0 Protocol described in RFC 2246
* **TLSv1_1**   : TLS 1.1 Protocol described in RFC 4346
* **TLSv1_2**   : TLS 1.2 Protocol described in RFC 5246
* **TLSv1_3**   : TLS 1.3 Protocol described in RFC 8446
* **TLS**       : Alias for TLS 1.2

> Note that TLSv1+ requires at least Python 2.7.9 or Python 3.4 built with OpenSSL 1.0.1+, and TLSv1_3 requires at least Python 2.7.15 or Python 3.7 built with OpenSSL 1.1.1+.

These protocol versions can be selected using the `hazelcast.config.PROTOCOL` as follows:

```python
from hazelcast.config import PROTOCOL

config.network_config.ssl_config.protocol = PROTOCOL.TLSv1_3
``` 

> Note that the Hazelcast Python client and the Hazelcast members should have the same protocol version in order for TLS/SSL to work. In case of the protocol mismatch, connection attempts will be refused.

Default value is `PROTOCOL.TLS` which is an alias for `PROTOCOL.TLSv1_2`.

##### Setting Cipher Suites

Cipher suites that will be used in the TLS/SSL communication can be set using the `ciphers` option. Cipher suites should be in the
OpenSSL cipher list format. More than one cipher suite can be set by separating them with a colon.

TLS/SSL implementation will honor the cipher suite order. So, Hazelcast Python client will offer the ciphers to the Hazelcast members with the given order. 

Note that, when this option is not set, all the available ciphers will be offered to the Hazelcast members with their default order.

The following is an example configuration:

```python
config.network_config.ssl_config.ciphers = "DHE-RSA-AES128-SHA:DHE-RSA-AES256-SHA"
``` 

#### 6.1.3. Mutual Authentication

As explained above, Hazelcast members have key stores used to identify themselves (to other members) and Hazelcast clients have trust stores used to define which members they can trust.

Using mutual authentication, the clients also have their key stores and members have their trust stores so that the members can know which clients they can trust.

To enable mutual authentication, firstly, you need to set the following property on the server side in the `hazelcast.xml` file:

```xml
<network>
    <ssl enabled="true">
        <properties>
            <property name="javax.net.ssl.mutualAuthentication">REQUIRED</property>
        </properties>
    </ssl>
</network>
```

You can see the details of setting mutual authentication on the server side in the [Mutual Authentication section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#mutual-authentication) of the Hazelcast IMDG Reference Manual.

On the client side, you have to provide `SSLConfig.cafile`, `SSLConfig.certfile` and `SSLConfig.keyfile` on top of the other TLS/SSL configurations. See the [TLS/SSL for Hazelcast Python Clients](#612-tlsssl-for-hazelcast-python-clients) for the details of these options.


# 7. Using Python Client with Hazelcast IMDG

This chapter provides information on how you can use Hazelcast IMDG's data structures in the Python client, after giving some basic information including an overview to the client API, operation modes of the client and how it handles the failures.


## 7.1. Python Client API Overview

Hazelcast Python client is designed to be fully asynchronous. See the [Basic Usage section](#15-basic-usage) to learn more about asynchronous nature of the Python Client.

If you are ready to go, let's start to use Hazelcast Python client.

The first step is configuration. You can configure the Python client programmatically. 

The following is an example on how to create a `ClientConfig` object and configure it programmatically:

```python
import hazelcast

config = hazelcast.ClientConfig()
config.group_config.name = "dev"
config.network_config.addresses.append("10.90.0.1")
```

The second step is initializing the `HazelcastClient` to be connected to the cluster:

```python
client = hazelcast.HazelcastClient(config)
```

**This client object is your gateway to access all the Hazelcast distributed objects.**

Let's create a map and populate it with some data, as shown below.

```python
customer_map = client.get_map("customers").blocking()
customer_map.put("1", "John Stiles")
customer_map.put("2", "Richard Miles")
customer_map.put("3", "Judy Doe")
```

As the final step, if you are done with your client, you can shut it down as shown below. This will release all the used resources and close connections to the cluster.

```python
client.shutdown()
```

## 7.2. Python Client Operation Modes

The client has two operation modes because of the distributed nature of the data and cluster: smart and unisocket.

### 7.2.1. Smart Client

In the smart mode, the clients connect to each cluster member. Since each data partition uses the well known and consistent hashing algorithm, each client can send an operation to the relevant cluster member, which increases the overall throughput and efficiency. Smart mode is the default mode.

### 7.2.2. Unisocket Client

For some cases, the clients can be required to connect to a single member instead of each member in the cluster. Firewalls, security or some custom networking issues can be the reason for these cases.

In the unisocket client mode, the client will only connect to one of the configured addresses. This single member will behave as a gateway to the other members. For any operation requested from the client, it will redirect the request to the relevant member and return the response back to the client returned from this member.

## 7.3. Handling Failures

There are two main failure cases you should be aware of. Below sections explain these and the configurations you can perform to achieve proper behavior.

### 7.3.1. Handling Client Connection Failure

While the client is trying to connect initially to one of the members in the `ClientNetworkConfig.addresses`, all the members might not be available. Instead of giving up, throwing an error and stopping the client, the client will retry as many as `connection_attempt_limit` times. 

You can configure `connection_attempt_limit` for the number of times you want the client to retry connecting. See the [Setting Connection Attempt Limit section](#55-setting-connection-attempt-limit).

The client executes each operation through the already established connection to the cluster. If this connection(s) disconnects or drops, the client will try to reconnect as configured.

### 7.3.2. Handling Retry-able Operation Failure

While sending the requests to the related members, the operations can fail due to various reasons. Read-only operations are retried by default. If you want to enable retrying for the other operations, you can set the `redo_operation` to `True`. See the [Enabling Redo Operation section](#53-enabling-redo-operation).

You can set a timeout for retrying the operations sent to a member. This can be provided by using the property `hazelcast.client.invocation.timeout.seconds` via `ClientConfig.set_property` method. The client will retry an operation within this given period, of course, if it is a read-only operation or you enabled the `redo_operation` as stated in the above paragraph. This timeout value is important when there is a failure resulted by either of the following causes:

* Member throws an exception.

* Connection between the client and member is closed.

* Client’s heartbeat requests are timed out.

When a connection problem occurs, an operation is retried if it is certain that it has not run on the member yet or if it is idempotent such as a read-only operation, i.e., retrying does not have a side effect. If it is not certain whether the operation has run on the member, then the non-idempotent operations are not retried. However, as explained in the first paragraph of this section, you can force all the client operations to be retried (`redo_operation`) when there is a connection failure between the client and member. But in this case, you should know that some operations may run multiple times causing conflicts. For example, assume that your client sent a `queue.offer` operation to the member and then the connection is lost. Since there will be no response for this operation, you will not know whether it has run on the member or not. If you enabled `redo_operation`, it means this operation may run again, which may cause two instances of the same object in the queue.

When invocation is being retried, the client may wait some time before it retries again. This duration can be configured using the following property:

 ```python
config.set_property("hazelcast.client.invocation.retry.pause.millis", 500)
```

The default retry wait time is `1` second.

## 7.4. Using Distributed Data Structures

Most of the distributed data structures are supported by the Python client. In this chapter, you will learn how to use these distributed data structures.

### 7.4.1. Using Map

Hazelcast Map is a distributed dictionary. Through the Python client, you can perform operations like reading and writing from/to a Hazelcast Map with the well known get and put methods. For details, see the [Map section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#map) in the Hazelcast IMDG Reference Manual.

A Map usage example is shown below.

```python
# Get the Distributed Map from Cluster.
my_map = client.get_map("my-distributed-map").blocking()

# Standard Put and Get
my_map.put("key", "value")
my_map.get("key")

# Concurrent Map methods, optimistic updating
my_map.put_if_absent("somekey", "somevalue") 
my_map.replace_if_same("key", "value", "newvalue")
```

### 7.4.2. Using MultiMap

Hazelcast MultiMap is a distributed and specialized map where you can store multiple values under a single key. For details, see the [MultiMap section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#multimap) in the Hazelcast IMDG Reference Manual.

A MultiMap usage example is shown below.

```python
# Get the Distributed MultiMap from Cluster.
multi_map = client.get_multi_map("my-distributed-multimap").blocking()

# Put values in the map against the same key
multi_map.put("my-key", "value1")
multi_map.put("my-key", "value2")
multi_map.put("my-key", "value3")

# Print out all the values for associated with key called "my-key"
# Outputs '['value2', 'value1', 'value3']'
values = multi_map.get("my-key") 
print(values) 

# Remove specific key/value pair
multi_map.remove("my-key", "value2") 
```

### 7.4.3. Using Replicated Map

Hazelcast Replicated Map is a distributed key-value data structure where the data is replicated to all members in the cluster. It provides full replication of entries to all members for high speed access. For details, see the [Replicated Map section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#replicated-map) in the Hazelcast IMDG Reference Manual.

A Replicated Map usage example is shown below.

```python
# Get a Replicated Map called "my-replicated-map"
replicated_map = client.get_replicated_map("my-replicated-map").blocking()

# Put and Get a value from the Replicated Map
# key/value replicated to all members
replaced_value = replicated_map.put("key", "value") 

# Will be None as its first update
print("replaced value = {}".format(replaced_value)) # Outputs 'replaced value = None'

# The value is retrieved from a random member in the cluster
value = replicated_map.get("key")

print("value for key = {}".format(value)) # Outputs 'value for key = value'
```

### 7.4.4. Using Queue

Hazelcast Queue is a distributed queue which enables all cluster members to interact with it. For details, see the [Queue section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#queue) in the Hazelcast IMDG Reference Manual.

A Queue usage example is shown below.

```python
# Get a Blocking Queue called "my-distributed-queue"
queue = client.get_queue("my-distributed-queue").blocking()

# Offer a String into the Distributed Queue
queue.offer("item")

# Poll the Distributed Queue and return the String
item = queue.poll()

# Timed blocking Operations
queue.offer("another-item", 1)
another_item = queue.poll(5)

# Indefinitely blocking Operations
queue.put("yet-another-item")

print(queue.take()) # Outputs 'yet-another-item'
```

### 7.4.5. Using Set

Hazelcast Set is a distributed set which does not allow duplicate elements. For details, see the [Set section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#set) in the Hazelcast IMDG Reference Manual.

A Set usage example is shown below.

```python
# Get the Distributed Set from Cluster.
my_set = client.get_set("my-distributed-set").blocking()

# Add items to the set with duplicates
my_set.add("item1")
my_set.add("item1")
my_set.add("item2")
my_set.add("item2")
my_set.add("item2")
my_set.add("item3")

# Get the items. Note that there are no duplicates.
for item in my_set.get_all():
    print(item)
```

### 7.4.6. Using List

Hazelcast List is a distributed list which allows duplicate elements and preserves the order of elements. For details, see the [List section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#list) in the Hazelcast IMDG Reference Manual.

A List usage example is shown below.

```python
# Get the Distributed List from Cluster.
my_list = client.get_list("my-distributed-list").blocking()

# Add element to the list
my_list.add("item1")
my_list.add("item2")

# Remove the first element
print("Removed: {}".format(my_list.remove_at(0))) # Outputs 'Removed: item1'

# There is only one element left
print("Current size is {}".format(my_list.size())) # Outputs 'Current size is 1'

# Clear the list
my_list.clear()
```

### 7.4.7. Using Ringbuffer

Hazelcast Ringbuffer is a replicated but not partitioned data structure that stores its data in a ring-like structure. You can think of it as a circular array with a given capacity. Each Ringbuffer has a tail and a head. The tail is where the items are added and the head is where the items are overwritten or expired. You can reach each element in a Ringbuffer using a sequence ID, which is mapped to the elements between the head and tail (inclusive) of the Ringbuffer. For details, see the [Ringbuffer section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#ringbuffer) in the Hazelcast IMDG Reference Manual.

A Ringbuffer usage example is shown below.

```python
# Get a RingBuffer called "my-ringbuffer"
ringbuffer = client.get_ringbuffer("my-ringbuffer").blocking()

# Add two items into ring buffer
ringbuffer.add(100)
ringbuffer.add(200)

# We start from the oldest item.
# If you want to start from the next item, call ringbuffer.tail_sequence()+1
sequence = ringbuffer.head_sequence()
print(ringbuffer.read_one(sequence)) # Outputs '100'

sequence += 1
print(ringbuffer.read_one(sequence)) # Outputs '200'
```

### 7.4.8. Using Topic

Hazelcast Topic is a distribution mechanism for publishing messages that are delivered to multiple subscribers. For details, see the [Topic section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#topic) in the Hazelcast IMDG Reference Manual.

A Topic usage example is shown below.

```python
# Function to be called when a message is published
def print_on_message(topic_message):
    print("Got message:", topic_message.message)

# Get a Topic called "my-distributed-topic"
topic = client.get_topic("my-distributed-topic")

# Add a Listener to the Topic
topic.add_listener(print_on_message)

# Publish a message to the Topic
topic.publish("Hello to distributed world") # Outputs 'Got message: Hello to distributed world'
```

### 7.4.9. Using Lock

Hazelcast Lock is a distributed lock implementation. You can synchronize Hazelcast members and clients using a `Lock`. For details, see the [Lock section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#lock) in the Hazelcast IMDG Reference Manual.

A Lock usage example is shown below.

```python
# Get a distributed lock called "my-distributed-lock"
lock = client.get_lock("my-distributed-lock").blocking()

# Now create a lock and execute some guarded code
lock.lock()
try:
    # Do something here
    pass
finally:
    lock.unlock()
```

### 7.4.10. Using Atomic Long

Hazelcast Atomic Long is the distributed long which offers most of the operations such as `get`, `set`, `get_and_set`, `compare_and_set` and `increment_and_get`. For details, see the [Atomic Long section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#iatomiclong) in the Hazelcast IMDG Reference Manual.

An Atomic Long usage example is shown below.

```python
# Get an Atomic Counter, we'll call it "counter"
counter = client.get_atomic_long("counter").blocking()

# Add and Get the "counter"
counter.add_and_get(3)  # value is 3

# Display the "counter" value
print("counter: {}".format(counter.get())) # Outputs 'counter: 3'
```

### 7.4.11. Using Semaphore

Hazelcast Semaphore is a distributed semaphore implementation. For details, see the [Semaphore section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#isemaphore) in the Hazelcast IMDG Reference Manual.

A Semaphore usage example is shown below.

```python
# Get a Semaphore called "my-distributed-semaphore"
semaphore = client.get_semaphore("my-distributed-semaphore").blocking()

# Initialize the Semaphore with 10 permits
semaphore.init(10)

# Acquire 5 permits
semaphore.acquire(5)

# Print the number of the available permits
print(semaphore.available_permits()) # Outputs '5'
```

### 7.4.12. Using Transactions

Hazelcast Python client provides transactional operations like beginning transactions, committing transactions and retrieving transactional data structures like the `TransactionalMap`, `TransactionalSet`, `TransactionalList`, `TransactionalQueue` and `TransactionalMultiMap`.

You can create a `Transaction` object using the Python client to begin, commit and rollback a transaction. You can obtain transaction-aware instances of queues, maps, sets, lists and multimaps via the `Transaction` object, work with them and commit or rollback in one shot. For details, see the [Transactions section](https://docs.hazelcast.org//docs/latest/manual/html-single/index.html#transactions) in the Hazelcast IMDG Reference Manual.

```python
# Create a Transaction object and begin the transaction
transaction = client.new_transaction(timeout=10)
transaction.begin()

# Get transactional distributed data structures
txn_map = transaction.get_map("transactional-map")
txn_queue = transaction.get_queue("transactional-queue")
txt_set = transaction.get_set("transactional-set")
try:
    obj = txn_queue.poll()

    # Process obj

    txn_map.put("1", "value1")
    txt_set.add("value")

    # Do other things
    
    # Commit the above changes done in the cluster.
    transaction.commit()  
except Exception as ex:
    # In the case of a transactional failure, rollback the transaction 
    transaction.rollback()
    print("Transaction failed! {}".format(ex.args))
```
In a transaction, operations will not be executed immediately. Their changes will be local to the `Transaction` object until committed. However, they will ensure the changes via locks.

For the above example, when `txn_map.put()` is executed, no data will be put in the map but the key will be locked against changes. While committing, operations will be executed, the value will be put to the map and the key will be unlocked.

The isolation level in Hazelcast Transactions is `READ_COMMITTED` on the level of a single partition. If you are in a transaction, you can read the data in your transaction and the data that is already committed. If you are not in a transaction, you can only read the committed data.

### 7.4.13. Using PN Counter

Hazelcast `PNCounter` (Positive-Negative Counter) is a CRDT positive-negative counter implementation. It is an eventually consistent counter given there is no member failure. For details, see the [PN Counter section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#pn-counter) in the Hazelcast IMDG Reference Manual.

A PN Counter usage example is shown below.

```python
# Get a PN Counter called 'pn-counter'
pn_counter = client.get_pn_counter("pn-counter").blocking()

# Counter is initialized with 0
print(pn_counter.get()) # 0

# .._and_get() variants does the operation
# and returns the final value
print(pn_counter.add_and_get(5))  # 5
print(pn_counter.decrement_and_get())  # 4

# get_and_..() variants returns the current 
# value and then does the operation
print(pn_counter.get_and_increment())  # 4
print(pn_counter.get())  # 5
```

### 7.4.14. Using Flake ID Generator

Hazelcast `FlakeIdGenerator` is used to generate cluster-wide unique identifiers. Generated identifiers are long 
primitive values and are k-ordered (roughly ordered). IDs are in the range from 0 to `2^63-1` (maximum signed long value). 
For details, see the [FlakeIdGenerator section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#flakeidgenerator) in the 
Hazelcast IMDG Reference Manual.

```python
# Get a Flake ID Generator called 'flake-id-generator'
generator = client.get_flake_id_generator("flake-id-generator").blocking()

# Generate a some unique identifier
print("ID: {}".format(generator.new_id()))
```

## 7.5. Distributed Events

This chapter explains when various events are fired and describes how you can add event listeners on a Hazelcast Python client. These events can be categorized as cluster and distributed data structure events.

### 7.5.1. Cluster Events

You can add event listeners to a Hazelcast Python client. You can configure the following listeners to listen to the events on the client side:

* Membership Listener: Notifies when a member joins to/leaves the cluster.

* Lifecycle Listener: Notifies when the client is starting, started, shutting down and shutdown.

#### 7.5.1.1. Listening for Member Events

You can add the following types of member events to the `ClusterService`.

* `member_added`: A new member is added to the cluster.
* `member_removed`: An existing member leaves the cluster.

The `ClusterService` class exposes an `add_listener()` method that allows one or more functions to be attached to the member events emitted by the class.

The following is a membership listener registration by using the `add_listener()` method.

```python
client.cluster.add_listener(member_added=lambda m: print("Member Added: The address is {}".format(m.address)))
```

#### 7.5.1.2. Listening for Lifecycle Events

The `Lifecycle Listener` notifies for the following events:

* `STARTING`: The client is starting.
* `CONNECTED`: The client is connected.
* `SHUTTING_DOWN`: The client is shutting down.
* `SHUTDOWN`: The client’s shutdown has completed.

The following is an example of the `Lifecycle listener` that is added to the `ClientConfig` object and its output.

```python
config.add_lifecycle_listener(lambda s: print("Lifecycle Event >>> {}".format(s)))

client = hazelcast.HazelcastClient(config)

client.shutdown()
```

**Output:**

```
Lifecycle Event >>> STARTING
Lifecycle Event >>> CONNECTED
Lifecycle Event >>> SHUTTING_DOWN
Lifecycle Event >>> SHUTDOWN
```

### 7.5.2. Distributed Data Structure Events

You can add event listeners to the distributed data structures.

#### 7.5.2.1. Listening for Map Events

You can listen to map-wide or entry-based events by attaching functions to the `Map` objects using the `add_entry_listener()` method. You can listen the following events.

* `added_func` : Function to be called when an entry is added to map.
* `removed_func` : Function to be called when an entry is removed from map.
* `updated_func` : Function to be called when an entry is updated. 
* `evicted_func` : Function to be called when an entry is evicted from map.
* `evict_all_func` : Function to be called when entries are evicted from map.
* `clear_all_func` : Function to be called when entries are cleared from map.
* `merged_func` : Function to be called when WAN replicated entry is merged.
* `expired_func` : Function to be called when an entry's live time is expired.

You can also filter the events using `key` or `predicate`. There is also an option called `include_value`. When this option is set to true, event will also include the value.

An entry-based event is fired after the operations that affect a specific entry. For example, `Map.put()`, `Map.remove()` or `Map.evict()`. An `EntryEvent` object is passed to the listener function.

See the following example.

```python
def added(event):
    print("Entry Added: {}-{}".format(event.key, event.value))
    
customer_map.add_entry_listener(include_value=True, added_func=added)
customer_map.put("4", "Jane Doe").result() # Outputs 'Entry Added: 4-Jane Doe'
```

A map-wide event is fired as a result of a map-wide operation. For example, `Map.clear()` or `Map.evict_all()`. An `EntryEvent` object is passed to the listener function.

See the following example.

```python
def cleared(event):
    print("Map Cleared: {}".format(event.number_of_affected_entries))
    
customer_map.add_entry_listener(include_value=True, clear_all_func=cleared)
customer_map.clear().result() # Outputs 'Map Cleared: 4'
```

## 7.6. Distributed Computing

This chapter explains how you can use Hazelcast IMDG's entry processor implementation in the Python client.

### 7.6.1. Using EntryProcessor

Hazelcast supports entry processing. An entry processor is a function that executes your code on a map entry in an atomic way.

An entry processor is a good option if you perform bulk processing on a `Map`. Usually you perform a loop of keys -- executing `Map.get(key)`, mutating the value, and finally putting the entry back in the map using `Map.put(key,value)`. If you perform this process from a client or from a member where the keys do not exist, you effectively perform two network hops for each update: the first to retrieve the data and the second to update the mutated value.

If you are doing the process described above, you should consider using entry processors. An entry processor executes a read and updates upon the member where the data resides. This eliminates the costly network hops described above.

> **NOTE: Entry processor is meant to process a single entry per call. Processing multiple entries and data structures in an entry processor is not supported as it may result in deadlocks on the server side.**

Hazelcast sends the entry processor to each cluster member and these members apply it to the map entries. Therefore, if you add more members, your processing completes faster.

#### Processing Entries

The `Map` class provides the following methods for entry processing:

* `execute_on_key` processes an entry mapped by a key.

* `execute_on_keys` processes entries mapped by a list of keys.

* `execute_on_entries` can process all entries in a map with a defined predicate. Predicate is optional.

In the Python client, an `EntryProcessor` should be `IdentifiedDataSerializable` or `Portable` because the server should be able to deserialize it to process.

The following is an example for `EntryProcessor` which is an `IdentifiedDataSerializable`.

```python
from hazelcast.serialization.api import IdentifiedDataSerializable

class IdentifiedEntryProcessor(IdentifiedDataSerializable):
    def __init__(self, value=None):
        self.value = value
    
    def read_data(self, object_data_input):
        self.value = object_data_input.read_utf()
    
    def write_data(self, object_data_output):
        object_data_output.write_utf(self.value)
    
    def get_factory_id(self):
        return 5
        
    def get_class_id(self):
        return 1
```

Now, you need to make sure that the Hazelcast member recognizes the entry processor. For this, you need to implement the Java equivalent of your entry processor and its factory, and create your own compiled class or JAR files. For adding your own compiled class or JAR files to the server's `CLASSPATH`, see the [Adding User Library to CLASSPATH section](#adding-user-library-to-classpath).

The following is the Java equivalent of the entry processor in Python client given above:

```java
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import java.io.IOException;
import java.util.Map;

public class IdentifiedEntryProcessor extends AbstractEntryProcessor<String, String> implements IdentifiedDataSerializable {
     static final int CLASS_ID = 1;
     private String value;
     
    public IdentifiedEntryProcessor() {
    }
    
     @Override
    public int getFactoryId() {
        return IdentifiedFactory.FACTORY_ID;
    }
    
     @Override
    public int getId() {
        return CLASS_ID;
    }
    
     @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(value);
    }
    
     @Override
    public void readData(ObjectDataInput in) throws IOException {
        value = in.readUTF();
    }
    
     @Override
    public Object process(Map.Entry<String, String> entry) {
        entry.setValue(value);
        return value;
    }
}
```

You can implement the above processor’s factory as follows:

```java
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class IdentifiedFactory implements DataSerializableFactory {
    public static final int FACTORY_ID = 5;
    
     @Override
    public IdentifiedDataSerializable create(int typeId) {
        if (typeId == IdentifiedEntryProcessor.CLASS_ID) {
            return new IdentifiedEntryProcessor();
        }
        return null;
    }
}
```

Now you need to configure the `hazelcast.xml` to add your factory as shown below.

```xml
<hazelcast>
    <serialization>
        <data-serializable-factories>
            <data-serializable-factory factory-id="5">
                IdentifiedFactory
            </data-serializable-factory>
        </data-serializable-factories>
    </serialization>
</hazelcast>
```

The code that runs on the entries is implemented in Java on the server side. The client side entry processor is used to specify which entry processor should be called. For more details about the Java implementation of the entry processor, see the [Entry Processor section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#entry-processor) in the Hazelcast IMDG Reference Manual.

After the above implementations and configuration are done and you start the server where your library is added to its `CLASSPATH`, you can use the entry processor in the `Map` methods. See the following example.

```python
distributed_map = client.get_map("my-distributed-map").blocking()

distributed_map.put("key", "not-processed")
distributed_map.execute_on_key("key", IdentifiedEntryProcessor("processed"))

print(distributed_map.get("key")) # Outputs 'processed'
```

## 7.7. Distributed Query

Hazelcast partitions your data and spreads it across cluster of members. You can iterate over the map entries and look for certain entries (specified by predicates) you are interested in. However, this is not very efficient because you will have to bring the entire entry set and iterate locally. Instead, Hazelcast allows you to run distributed queries on your distributed map.

### 7.7.1. How Distributed Query Works

1. The requested predicate is sent to each member in the cluster.
2. Each member looks at its own local entries and filters them according to the predicate. At this stage, key-value pairs of the entries are deserialized and then passed to the predicate.
3. The predicate requester merges all the results coming from each member into a single set.

Distributed query is highly scalable. If you add new members to the cluster, the partition count for each member is reduced and thus the time spent by each member on iterating its entries is reduced. In addition, the pool of partition threads evaluates the entries concurrently in each member, and the network traffic is also reduced since only filtered data is sent to the requester.

**Predicate Module Operators**

The `Predicate` module offered by the Python client includes many operators for your query requirements. Some of them are explained below.

* `is_equal_to`: Checks if the result of an expression is equal to a given value.
* `is_not_equal_to`: Checks if the result of an expression is not equal to a given value.
* `is_instance_of`: Checks if the result of an expression has a certain type.
* `is_like`: Checks if the result of an expression matches some string pattern. `%` (percentage sign) is the placeholder for many characters, `_` (underscore) is placeholder for only one character.
* `is_ilike`: Checks if the result of an expression matches some string pattern in a case-insensitive manner.
* `is_greater_than`: Checks if the result of an expression is greater than a certain value.
* `is_greater_than_or_equal_to`: Checks if the result of an expression is greater than or equal to a certain value.
* `is_less_than`: Checks if the result of an expression is less than a certain value.
* `is_less_than_or_equal_to`: Checks if the result of an expression is less than or equal to a certain value.
* `is_between`: Checks if the result of an expression is between two values (this is inclusive).
* `is_in`: Checks if the result of an expression is an element of a certain list.
* `is_not`: Checks if the result of an expression is false.
* `matches_regex`: Checks if the result of an expression matches some regular expression.
* `true`: Creates an always true predicate that will pass all items.
* `false`: Creates an always false predicate that will filter out all items.

Hazelcast offers the following ways for distributed query purposes:

* Combining Predicates with AND, OR, NOT

* Distributed SQL Query

#### 7.7.1.1. Employee Map Query Example

Assume that you have an `employee` map containing the instances of `Employee` class, as coded below. 

```python
from hazelcast.serialization.api import Portable

class Employee(Portable):
    def __init__(self, name=None, age=None, active=None, salary=None):
        self.name = name
        self.age = age
        self.active = active
        self.salary = salary
    
    def get_class_id(self):
        return 100
    
    def get_factory_id(self):
        return 1000
    
    def read_portable(self, reader):
        self.name = reader.read_utf("name")
        self.age = reader.read_int("age")
        self.active = reader.read_boolean("active")
        self.salary = reader.read_double("salary")
    
    def write_portable(self, writer):
        writer.write_utf("name", self.name)
        writer.write_int("age", self.age)
        writer.write_boolean("active", self.active)
        writer.write_double("salary", self.salary)
```

Note that `Employee` extends `Portable`. As portable types are not deserialized on the server side for querying, you don’t need to implement its Java equivalent on the server side.

For types that are not portable, you need to implement its Java equivalent and its data serializable factory on the server side for server to reconstitute the objects from binary formats. In this case, you need to compile the `Employee` and related factory classes with server's `CLASSPATH` and add them to the `user-lib` directory in the extracted `hazelcast-<version>.zip` (or `tar`) before starting the server. See the [Adding User Library to CLASSPATH section](#adding-user-library-to-classpath).

> **NOTE: Querying with `Portable` class is faster as compared to `IdentifiedDataSerializable`.**

#### 7.7.1.2. Querying by Combining Predicates with AND, OR, NOT

You can combine predicates by using the `and_`, `or_` and `not_` operators, as shown in the below example.

```python
from hazelcast.serialization.predicate import and_, is_equal_to, is_less_than

employee_map = client.get_map("employee")

predicate = and_(is_equal_to('active', True), is_less_than('age', 30))

employees = employee_map.values(predicate).result()
```

In the above example code, `predicate` verifies whether the entry is active and its `age` value is less than 30. This `predicate` is applied to the `employee` map using the `Map.values` method. This method sends the predicate to all cluster members and merges the results coming from them. 

> **NOTE: Predicates can also be applied to `key_set` and `entry_set` of the Hazelcast IMDG's distributed map.**

#### 7.7.1.3. Querying with SQL

`SqlPredicate` takes the regular SQL `where` clause. See the following example:

```python
from hazelcast.serialization.predicate import sql

employee_map = client.get_map("employee")

employees = employee_map.values(sql("active AND age < 30")).result()
```

##### Supported SQL Syntax

**AND/OR:** `<expression> AND <expression> AND <expression>…`
   
- `active AND age > 30`
- `active = false OR age = 45 OR name = 'Joe'`
- `active AND ( age > 20 OR salary < 60000 )`

**Equality:** `=, !=, <, ⇐, >, >=`

- `<expression> = value`
- `age <= 30`
- `name = 'Joe'`
- `salary != 50000`

**BETWEEN:** `<attribute> [NOT] BETWEEN <value1> AND <value2>`

- `age BETWEEN 20 AND 33 ( same as age >= 20 AND age ⇐ 33 )`
- `age NOT BETWEEN 30 AND 40 ( same as age < 30 OR age > 40 )`

**IN:** `<attribute> [NOT] IN (val1, val2,…)`

- `age IN ( 20, 30, 40 )`
- `age NOT IN ( 60, 70 )`
- `active AND ( salary >= 50000 OR ( age NOT BETWEEN 20 AND 30 ) )`
- `age IN ( 20, 30, 40 ) AND salary BETWEEN ( 50000, 80000 )`

**LIKE:** `<attribute> [NOT] LIKE 'expression'`

The `%` (percentage sign) is the placeholder for multiple characters, an `_` (underscore) is the placeholder for only one character.

- `name LIKE 'Jo%'` (true for 'Joe', 'Josh', 'Joseph' etc.)
- `name LIKE 'Jo_'` (true for 'Joe'; false for 'Josh')
- `name NOT LIKE 'Jo_'` (true for 'Josh'; false for 'Joe')
- `name LIKE 'J_s%'` (true for 'Josh', 'Joseph'; false 'John', 'Joe')

**ILIKE:** `<attribute> [NOT] ILIKE 'expression'`

ILIKE is similar to the LIKE predicate but in a case-insensitive manner.

- `name ILIKE 'Jo%'` (true for 'Joe', 'joe', 'jOe','Josh','joSH', etc.)
- `name ILIKE 'Jo_'` (true for 'Joe' or 'jOE'; false for 'Josh')

**REGEX:** `<attribute> [NOT] REGEX 'expression'`

- `name REGEX 'abc-.*'` (true for 'abc-123'; false for 'abx-123')

##### Querying Examples with Predicates

You can use the `__key` attribute to perform a predicated search for the entry keys. See the following example:

```python
from hazelcast.serialization.predicate import sql

person_map = client.get_map("persons").blocking()

person_map.put("John", 28)
person_map.put("Mary", 23)
person_map.put("Judy", 30)

predicate = sql("__key like M%")

persons = person_map.values(predicate)

print(persons[0]) # Outputs '23'
```

In this example, the code creates a list with the values whose keys start with the letter "M”.

You can use the `this` attribute to perform a predicated search for the entry values. See the following example:

```python
from hazelcast.serialization.predicate import is_greater_than_or_equal_to

person_map = client.get_map("persons").blocking()

person_map.put("John", 28)
person_map.put("Mary", 23)
person_map.put("Judy", 30)

predicate = is_greater_than_or_equal_to("this", 27)

persons = person_map.values(predicate)

print(persons[0], persons[1]) # Outputs '28 30'
```

In this example, the code creates a list with the values greater than or equal to "27".

#### 7.7.1.4. Querying with JSON Strings

You can query JSON strings stored inside your Hazelcast clusters. To query the JSON string,
you first need to create a `HazelcastJsonValue` from the JSON string or JSON serializable object.
You can use ``HazelcastJsonValue``s both as keys and values in the distributed data structures. Then, it is
possible to query these objects using the Hazelcast query methods explained in this section.

```python
from hazelcast.core import HazelcastJsonValue
person1 = "{ \"name\": \"John\", \"age\": 35 }"
person2 = "{ \"name\": \"Jane\", \"age\": 24 }"
person3 = {"name": "Trey", "age": 17}

id_person_map = client.get_map("json-values").blocking()

# From JSON string
id_person_map.put(1, HazelcastJsonValue(person1))
id_person_map.put(2, HazelcastJsonValue(person2))

# From JSON serializable object
id_person_map.put(3, HazelcastJsonValue(person3))

people_under_21 = id_person_map.values(is_less_than("age", 21)) 
```

When running the queries, Hazelcast treats values extracted from the JSON documents as Java types so they
can be compared with the query attribute. JSON specification defines five primitive types to be used in the JSON
documents: `number`,`string`, `true`, `false` and `null`. The `string`, `true/false` and `null` types are treated
as `String`, `boolean` and `null`, respectively. We treat the extracted `number` values as `long`s if they
can be represented by a `long`. Otherwise, `number`s are treated as `double`s.

It is possible to query nested attributes and arrays in the JSON documents. The query syntax is the same
as querying other Hazelcast objects using the `Predicate`s.`


```python
# Sample JSON object
# {
#     "departmentId": 1,
#     "room": "alpha",
#     "people": [
#         {
#             "name": "Peter",
#             "age": 26,
#             "salary": 50000
#         },
#         {
#             "name": "Jonah",
#             "age": 50,
#             "salary": 140000
#         }
#     ]
# }
# The following query finds all the departments that have a person named "Peter" working in them.

department_with_peter = departments.values(is_equal_to("people[any].name", "Peter"))
```

`HazelcastJsonValue` is a lightweight wrapper around your JSON strings. It is used merely as a way to indicate that the contained string should be treated as a valid JSON value. Hazelcast does not check the validity of JSON strings put into to the maps. Putting an invalid JSON string into a map is permissible. However, in that case whether such an entry is going to be returned or not from a query is not defined.

## 7.8. Performance

### 7.8.1. Near Cache

Map entries in Hazelcast are partitioned across the cluster members. Hazelcast clients do not have local data at all. Suppose you read the key `k` a number of times from a Hazelcast client and `k` is owned by a member in your cluster. Then each `map.get(k)` will be a remote operation, which creates a lot of network trips. If you have a map that is mostly read, then you should consider creating a local Near Cache, so that reads are sped up and less network traffic is created. 

These benefits do not come for free, please consider the following trade-offs:

- Clients with a Near Cache will have to hold the extra cached data, which increases memory consumption.
- If invalidation is enabled and entries are updated frequently, then invalidations will be costly.
- Near Cache breaks the strong consistency guarantees; you might be reading stale data.
 
Near Cache is highly recommended for maps that are mostly read.

#### 7.8.1.1. Configuring Near Cache

The following snippet show how a Near Cache is configured in the Python client, presenting all available values for each element:

```python
from hazelcast.config import NearCacheConfig, IN_MEMORY_FORMAT, EVICTION_POLICY

near_cache_config = NearCacheConfig("mostly-read-map")
near_cache_config.invalidate_on_change = False
near_cache_config.time_to_live_seconds = 600
near_cache_config.max_idle_seconds = 5
near_cache_config.in_memory_format = IN_MEMORY_FORMAT.OBJECT
near_cache_config.eviction_policy = EVICTION_POLICY.LRU
near_cache_config.eviction_max_size = 100
near_cache_config.eviction_sampling_count = 8
near_cache_config.eviction_sampling_pool_size = 16

config.add_near_cache_config(near_cache_config)
```

Following are the descriptions of all configuration elements:

- `in_memory_format`: Specifies in which format data will be stored in your Near Cache. Note that a map’s in-memory format can be different from that of its Near Cache. Available values are as follows:
  - `BINARY`: Data will be stored in serialized binary format (default value).
  - `OBJECT`: Data will be stored in deserialized format.
- `invalidate_on_change`: Specifies whether the cached entries are evicted when the entries are updated or removed. Its default value is `True`.
- `time_to_live_seconds`: Maximum number of seconds for each entry to stay in the Near Cache. Entries that are older than this period are automatically evicted from the Near Cache. Regardless of the eviction policy used, `time_to_live_seconds` still applies. Any non-negative number can be assigned. Its default value is `None`. `None` means infinite. 
- `max_idle_seconds`: Maximum number of seconds each entry can stay in the Near Cache as untouched (not read). Entries that are not read more than this period are removed from the Near Cache. Any non-negative number can be assigned. Its default value is `None`. `None` means infinite. 
- `eviction_policy`: Eviction policy configuration. Available values are as follows:
  - `LRU`: Least Recently Used (default value).
  - `LFU`: Least Frequently Used.
  - `NONE`: No items are evicted and the `eviction_max_size` property is ignored. You still can combine it with `time_to_live_seconds` and `max_idle_seconds` to evict items from the Near Cache. 
  - `RANDOM`: A random item is evicted.
- `eviction_max_size`: Maximum number of entries kept in the memory before eviction kicks in.
- `eviction_sampling_count`: Number of random entries that are evaluated to see if some of them are already expired. If there are expired entries, those are removed and there is no need for eviction.
- `eviction_sampling_pool_size`: Size of the pool for eviction candidates. The pool is kept sorted according to eviction policy. The entry with the highest score is evicted. 

#### 7.8.1.2. Near Cache Example for Map

The following is an example configuration for a Near Cache defined in the `mostly-read-map` map. According to this configuration, the entries are stored as `OBJECT`'s in this Near Cache and eviction starts when the count of entries reaches `5000`; entries are evicted based on the `LRU` (Least Recently Used) policy. In addition, when an entry is updated or removed on the member side, it is eventually evicted on the client side.

```python
near_cache_config = NearCacheConfig("mostly-read-map")
near_cache_config.invalidate_on_change = True
near_cache_config.in_memory_format = IN_MEMORY_FORMAT.OBJECT
near_cache_config.eviction_policy = EVICTION_POLICY.LRU
near_cache_config.eviction_max_size = 5000

config.add_near_cache_config(near_cache_config)
```

#### 7.8.1.3. Near Cache Eviction

In the scope of Near Cache, eviction means evicting (clearing) the entries selected according to the given `eviction_policy` when the specified `eviction_max_size` has been reached.

The `eviction_max_size` defines the entry count when the Near Cache is full and determines whether the eviction should be triggered. 

Once the eviction is triggered, the configured `eviction_policy` determines which, if any, entries must be evicted.

#### 7.8.1.4. Near Cache Expiration

Expiration means the eviction of expired records. A record is expired:

- If it is not touched (accessed/read) for `max_idle_seconds`
- `time_to_live_seconds` passed since it is put to Near Cache

The actual expiration is performed when a record is accessed: it is checked if the record is expired or not. If it is expired, it is evicted and `KeyError` is raised to the caller.

#### 7.8.1.5. Near Cache Invalidation

Invalidation is the process of removing an entry from the Near Cache when its value is updated or it is removed from the original map (to prevent stale reads). See the [Near Cache Invalidation section](https://docs.hazelcast.org/docs/latest/manual/html-single/#near-cache-invalidation) in the Hazelcast IMDG Reference Manual.

## 7.9. Monitoring and Logging

### 7.9.1. Enabling Client Statistics

You can monitor your clients using Hazelcast Management Center.

As a prerequisite, you need to enable the client statistics before starting your clients. This can be done by setting the `hazelcast.client.statistics.enabled` system property to `true` on the **member** as the following:

```xml
<hazelcast>
    ...
    <properties>
        <property name="hazelcast.client.statistics.enabled">true</property>
    </properties>
    ...
</hazelcast>
```

Also, you need to enable the client statistics in the Python client. There are two properties related to client statistics:

- `hazelcast.client.statistics.enabled`: If set to `True`, it enables collecting the client statistics and sending them to the cluster. When it is `True` you can monitor the clients that are connected to your Hazelcast cluster, using Hazelcast Management Center. Its default value is `False`.

- `hazelcast.client.statistics.period.seconds`: Period in seconds the client statistics are collected and sent to the cluster. Its default value is `3`.

You can enable client statistics and set a non-default period in seconds as follows:

```python
config = hazelcast.ClientConfig()
config.set_property(ClientProperties.STATISTICS_ENABLED.name, True)
config.set_property(ClientProperties.STATISTICS_PERIOD_SECONDS.name, 4)
```

Hazelcast Python client can collect statistics related to the client and Near Caches without an extra dependency. However, to get the statistics about the runtime and operating system, [psutil](https://pypi.org/project/psutil/) is used as an extra dependency.

If the `psutil` is installed, runtime and operating system statistics will be sent to cluster along with statistics related to the client and Near Caches. If not, only the client and Near Cache statistics will be sent.

`psutil` can be installed independently or with the Hazelcast Python client as follows:

**From PyPI**
```
pip install hazelcast-python-client[stats]
```

**From source**

```
pip install -e .[stats]
```
 
After enabling the client statistics, you can monitor your clients using Hazelcast Management Center. Please refer to the [Monitoring Clients section](https://docs.hazelcast.org/docs/management-center/latest/manual/html/index.html#monitoring-clients) in the Hazelcast Management Center Reference Manual for more information on the client statistics.

### 7.9.2 Logging Configuration

Hazelcast Python client allows you to configure the logging through the `LoggerConfig` in the `ClientConfig` class. 

`LoggerConfig` contains options that allow you to set the logging level and a custom logging configuration file to the Hazelcast Python client. 

By default, Hazelcast Python client will log to the `sys.stderr` with the `INFO` logging level and `%(asctime)s %(name)s\n%(levelname)s: %(version_message)s %(message)s` format where the `version_message` contains the information about the client version, group name and client name.

Below is an example of the default logging configuration.

```python
import hazelcast

client = hazelcast.HazelcastClient()
client.shutdown()
```

**Output to the `sys.stderr`**
```
Feb 15, 2019 12:57:13 PM HazelcastClient
INFO: [3.10] [dev] [hz.client_0] A non-empty group password is configured for the Hazelcast client. Starting with Hazelcast IMDG version 3.11, clients with the same group name, but with different group passwords (that do not use authentication) will be accepted to a cluster. The group password configuration will be removed completely in a future release.
Feb 15, 2019 12:57:13 PM HazelcastClient.LifecycleService
INFO: [3.10] [dev] [hz.client_0] (20181119 - 9080a46) HazelcastClient is STARTING
Feb 15, 2019 12:57:13 PM HazelcastClient.ClusterService
INFO: [3.10] [dev] [hz.client_0] Connecting to Address(host=127.0.0.1, port=5701)
Feb 15, 2019 12:57:13 PM HazelcastClient.ConnectionManager
INFO: [3.10] [dev] [hz.client_0] Authenticated with Connection(address=('127.0.0.1', 5701), id=0)
Feb 15, 2019 12:57:13 PM HazelcastClient.ClusterService
INFO: [3.10] [dev] [hz.client_0] New member list:

Members [1] {
	Member [10.216.1.49]:5701 - 1f4bb35d-b68f-46eb-bd65-61e3f4bc9922
}

Feb 15, 2019 12:57:13 PM HazelcastClient.LifecycleService
INFO: [3.10] [dev] [hz.client_0] (20181119 - 9080a46) HazelcastClient is CONNECTED
Feb 15, 2019 12:57:13 PM HazelcastClient
INFO: [3.10] [dev] [hz.client_0] Client started.
Feb 15, 2019 12:57:13 PM HazelcastClient.LifecycleService
INFO: [3.10] [dev] [hz.client_0] (20181119 - 9080a46) HazelcastClient is SHUTTING_DOWN
Feb 15, 2019 12:57:13 PM HazelcastClient.AsyncoreReactor
WARNING: [3.10] [dev] [hz.client_0] Connection closed by server
Feb 15, 2019 12:57:13 PM HazelcastClient.LifecycleService
INFO: [3.10] [dev] [hz.client_0] (20181119 - 9080a46) HazelcastClient is SHUTDOWN
Feb 15, 2019 12:57:13 PM HazelcastClient
INFO: [3.10] [dev] [hz.client_0] Client shutdown.
```

Let's go over the `LoggerConfig` options one by one.

#### Setting Logging Level

Although you can not change the logging levels used within the Hazelcast Python client, you can specify a logging level that is used to threshold the logs that are at least as severe as your specified level using `ClientConfig.logger_config.level`.

Here is the table listing the default logging levels that come with the `logging` module and numeric values that represent their severity:

| Level    | Numeric Value |
|----------|---------------|
| CRITICAL | 50            |
| ERROR    | 40            |
| WARNING  | 30            |
| INFO     | 20            |
| DEBUG    | 10            |
| NOTSET   | 0             |

For example, setting the logging level to `logging.DEBUG` will cause all the logging messages that are equal or higher than the `logging.DEBUG` in terms of severity to be emitted by your logger.

By default, the logging level is set to `logging.INFO`.

To turn off the logging, you can set `ClientConfig.logger_config.level` to a value greater than the numeric value of `logging.CRITICAL`. For example, the configuration below turns off the logging for the Hazelcast Python client.

```python
config.logger_config.level = 100  # Any value greater than 50 will turn off the logging
client = hazelcast.HazelcastClient(config)
``` 

#### Setting a Custom Logging Configuration

`ClientConfig.logger_config.config_file` can be used to configure the logger for the Hazelcast Python client entirely.
 
When set, this field should contain the absolute path of the JSON file that contains the logging configuration as described in the [Configuration dictionary schema](https://docs.python.org/3/library/logging.config.html#logging-config-dictschema). This file will be read and the contents of it will be directly fed into the `logging.dictConfig` function.

When this field is set, the `level` field is simply discarded and configuration in this file is used.

All Hazelcast Python client related loggers have `HazelcastClient` as their parent logger. So, you can configure logging for the `HazelcastClient` base logger and this logging configuration can be used for all client related loggers. 

Let's replicate the default configuration used within the Hazelcast client with this configuration method.

**config.json**
```json
{
    "version": 1,
    "disable_existing_loggers": false,
    "filters": {
        "version_message_filter": {
            "()": "some_package.log.VersionMessageFilter"
        }
    },
    "formatters": {
        "hazelcast_formatter": {
            "()": "some_package.log.HazelcastFormatter",
            "format": "%(asctime)s %(name)s\n%(levelname)s: %(version_message)s %(message)s",
            "datefmt": "%b %d, %Y %I:%M:%S %p"
        }
    },
    "handlers": {
        "console_handler": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "filters": ["version_message_filter"],
            "formatter": "hazelcast_formatter"
        }
    },
    "loggers": {
        "HazelcastClient": {
            "handlers": ["console_handler"],
            "level": "INFO"
        }
    }
}
```

**some_package/log.py**
```python
import logging

from hazelcast.version import CLIENT_VERSION

class VersionMessageFilter(logging.Filter):
    def filter(self, record):
        record.version_message = "[" + CLIENT_VERSION + "]"
        return True
        
class HazelcastFormatter(logging.Formatter):
    def format(self, record):
        client_name = getattr(record, "client_name", None)
        group_name = getattr(record, "group_name", None)
        if client_name and group_name:
            record.msg = "[" + group_name + "] [" + client_name + "] " + record.msg
        return super(HazelcastFormatter, self).format(record)
```

**some_package/test.py**
```python
import hazelcast

config = hazelcast.ClientConfig()
config.logger_config.config_file = "/home/hazelcast/config.json"

client = hazelcast.HazelcastClient(config)

## Some operations

client.shutdown()
``` 

To learn more about the `logging` module and its capabilities, please see the [logging cookbook](https://docs.python.org/3/howto/logging-cookbook.html) and [documentation](https://docs.python.org/3/library/logging.html) of the `logging` module.

# 8. Development and Testing

If you want to help with bug fixes, develop new features or tweak the implementation to your application's needs, you can follow the steps in this section.

## 8.1. Building and Using Client From Sources

Follow the below steps to build and install Hazelcast Python client from its source:

1. Clone the GitHub repository (https://github.com/hazelcast/hazelcast-python-client.git).
2. Run `python setup.py install` to install the Python client.

If you are planning to contribute, please make sure that it fits the guidelines described in [PEP8](https://www.python.org/dev/peps/pep-0008/).

## 8.2. Testing

In order to test Hazelcast Python client locally, you will need the following:

* Java 6 or newer
* Maven

Following commands starts the tests according to your operating system:

```bash
sh run-tests.sh
```

or 

```
PS> .\run-tests.ps1
```

Test script automatically downloads `hazelcast-remote-controller` and Hazelcast IMDG. The script uses Maven to download those.

# 9. Getting Help

You can use the following channels for your questions and development/usage issues:

* This repository by opening an issue.
* Our Google Groups directory: https://groups.google.com/forum/#!forum/hazelcast
* Stack Overflow: https://stackoverflow.com/questions/tagged/hazelcast

# 10. Contributing

Besides your development contributions as explained in the [Development and Testing chapter](#8-development-and-testing) above, you can always open a pull request on this repository for your other requests.

# 11. License

[Apache 2 License](https://github.com/hazelcast/hazelcast-python-client/blob/master/LICENSE.txt).

# 12. Copyright

Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com) for more information.
