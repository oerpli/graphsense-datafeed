import os
import pickle
import time
from argparse import ArgumentParser
from multiprocessing import Pool, Value
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

class QueryManager(object):
    def __init__(self, cluster, keyspace):
        self.cluster = Cluster([cluster])
        self.session = self.cluster.connect()
        self.session.default_timeout = 60  # maybe a problem with huge insert?
        self.session.set_keyspace(keyspace)
        cql_stmt = """COPY ? FROM ? WITH HEADER = true;"""
        self.copy_from_csv_stmt = self.session.prepare(cql_stmt)

    def copyFromCsv(self,filename, table):
        print("Copying {} to {}".format(filename,table))
        while True:
            batchStmt = BatchStatement()
            batchStmt.add(self.copy_from_csv_stmt, (table,filename))
            while True:
                try:
                    self.session.execute(batchStmt)
                except Exception as err:
                    print("Exception ", err, " retrying...", end="\r")
                    continue
                break

def main():
    parser = ArgumentParser()
    parser.add_argument("-c", "--cassandra", dest="cassandra",
                        help="cassandra node",
                        default="localhost")
    parser.add_argument("-k", "--keyspace", dest="keyspace",
                        help="keyspace to import data to",
                        default="graphsense_raw_xmr")
    # parser.add_argument("-p", "--processes", dest="num_proc",
    #                     type=int, default=1,
    #                     help="number of processes")
    parser.add_argument("-i", "--inputs", dest="inputs", required=True,
                        help="(absolute?) path of inputs.csv file")
    parser.add_argument("-o", "--outputs", dest="outputs", required=True,
                        help="(absolute?) path of outputs.csv file")

    args = parser.parse_args()

    # files = [os.path.join(args.directory, f)
    #          for f in os.listdir(args.directory)
    #          if os.path.isfile(os.path.join(args.directory, f))]

    qm = QueryManager(args.cassandra, args.keyspace)
    start = time.time()
    qm.copyFromCsv(args.outputs,"outputs")
    qm.copyFromCsv(args.inputs,"inputs")
    delta = time.time() - start
    print("\n%.1fs" % delta)


if __name__ == "__main__":
    main()
