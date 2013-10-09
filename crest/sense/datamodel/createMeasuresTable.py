__author__ = 'chriswhsu'

from crest.sense.senseworker import SenseWorker

sns = SenseWorker(test=False)

# sns.session.execute("""drop table if exists measures""")
# sns.log.info('dropped table measures.')

sns.session.execute("""
                        CREATE TABLE measures (
                          name text,
                          description text,
                          uom text,
                          datatype text,
                          PRIMARY KEY (name)
                        ) WITH
                          compression={'sstable_compression': 'SnappyCompressor'};
                    """)

sns.log.info('created table measures.')