CREATE KEYSPACE if not exists sense
     WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};