from cassandra.cluster import Cluster

# Define the Cassandra host address
host = "35.244.104.22"

# Create a Cluster object and connect to Cassandra
cluster = Cluster([host])

# Create a session to execute queries
session = cluster.connect()

# Use the 'reddit' keyspace
keyspace = "reddit"
session.set_keyspace(keyspace)

# Fetch and print all tables in the keyspace
tables = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s", (keyspace,))
for row in tables:
    print(row.table_name)

# Don't forget to close the session and cluster when you're done
session.shutdown()
cluster.shutdown()
