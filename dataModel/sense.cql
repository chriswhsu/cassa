CREATE KEYSPACE sense
     WITH replication = {'class': 'NetworkTopologyStrategy', 'UCB-HEARST' : 2};