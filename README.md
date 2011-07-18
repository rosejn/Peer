## Peer to peer networking toolkit

### Providing
* TCP and UDP based, NIO socket communications
* a simple communications interface (RPC, multiplexed streams)
* UPnP based port-forwarding configuration
 - makes peers behind home networks (NATs) accessible from the outside
* high-level functions for message and password hashing
* automatic local network discovery using UDP broadcast messages

### In Progress
* generic bootstrap server for P2P apps
* heartbeat based failure detection to recognize dropped or unavailable peers
* random walk based search and sampling
* localized flooding to peers
* distributed hash table formation
 - ring based topology
* generic greedy routing
 - for ring based DHT ala chord and others
* basic distributed clustering

### Planned
* high-level DHT storage interface
* content based routing using graph queries
