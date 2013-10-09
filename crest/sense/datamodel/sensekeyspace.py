__author__ = 'chriswhsu'


from crest.sense.senseworker import SenseWorker

sns = SenseWorker(test=True)

sns.session.execute("""

                    CREATE KEYSPACE if not exists sense
                         WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 2}
                    """)

sns.log.info('confirmed / created sense keyspace.')