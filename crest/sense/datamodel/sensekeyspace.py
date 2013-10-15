__author__ = 'chriswhsu'


from crest.sense.senseworker import SenseWorker

sns = SenseWorker(test=True)

sns.session.execute("""

                    CREATE KEYSPACE sense
                         WITH replication = {'class': 'NetworkTopologyStrategy', 'UCB-HEARST' : 2}
                    """)

sns.log.info('confirmed / created sense keyspace.')