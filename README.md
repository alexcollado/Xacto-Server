# Xacto-Server
A multi-threaded network server that implements a transactional object store.
This was the final project for my Systems Architecture course, where the goal
was to become familiar with thread execution, locks, semaphores, and gain a basic
understanding of socket programming.

The following overview is taken from the project documentation and describes how
the server works.

## The Xacto Server and Protocol: Overview

The "Xacto" server implements a simple transactional object store,
designed for use in a network environment. For the purposes of this
assignment, an object store is essentially a hash map that maps
**keys** to **values**, where both keys and values can be arbitrary data.
There are two basic operations on the store: `PUT` and `GET`.
As you might expect, the `PUT` operation associates a key with a value,
and the `GET` operation retrieves the value associated with a key.
Although values may be null, keys may not.  The null value is used as
the default value associated with a key if no `PUT` has been performed
on that key.

A client wishing to make use of the object store first makes a network
connection to the server.  Upon connection, a **transaction** is
created by the server.  All operations that the client performs during
this session will execute in the scope of that transaction.
The client issues a series of `PUT` and `GET` requests to the server,
receiving a response from each one.  Upon completing the series of
requests, the client issues a `COMMIT` request.  If all is well, the
transaction **commits** and its effects on the store are made permanent.
Due to interference of transactions being executed concurrently by
other clients, however, it is possible for a transaction to **abort**
rather than committing.  The effects of an aborted transaction are
erased from the store and it is as if the transaction had never happened.
A transaction may abort at any time up until it has successfully committed;
in particular the response from any `PUT` or `GET` request might
indicate that the transaction has aborted.  When a transaction has
aborted, the server closes the client connection.  The client may
then open a new connection and retry the operations in the context of
a new transaction.

The Xacto server architecture is that of a multi-threaded network
server.  When the server is started, a **master** thread sets up a
socket on which to listen for connections from clients.  When a
connection is accepted, a **client service thread** is started to
handle requests sent by the client over that connection.  The client
service thread executes a service loop in which it repeatedly receives
a **request packet** sent by the client, performs the request, and
sends a **reply** packet that indicates the result.
Besides request and reply packets, there are also **data** packets
that are used to send key and value data between the client and
server.
