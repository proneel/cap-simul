# cap-simul
A simulation of the tradeoffs defined by Brewer's conjecture (or [CAP theorem](https://en.wikipedia.org/wiki/CAP_theorem)) with strong concurrency demonstrated with [Paxos](http://research.microsoft.com/en-us/um/people/lamport/pubs/paxos-simple.pdf) &amp; [Raft](https://raft.github.io/) and eventual consistency demonstrated by [Dynamo](http://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf).

This project has probably little practical use to any one except to visualize how CP (consistent, partition-tolerant) and AP (available, partition-tolerant) systems tradeoffs look like when updating distributed state. Only developers of distributed data stores or message oriented middleware need worry about implementing such algorithms. And even then, [epaxos](https://github.com/go-distributed/epaxos) and [raft](https://github.com/hashicorp/raft) are production ready (?) libraries that will helpfully implement the hard bits.

I wrote this only to learn Go in the context of what its strong suit is (highy concurrency server side programming).

Current state:
A trivial and inefficient paxos implementation.
