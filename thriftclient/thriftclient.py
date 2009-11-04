from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated 

import time, random

class NoServersAvailable(Exception): pass

class ThriftClient(object):
    """Thirft Client

    Connects to a Thrift client and runs the available attributes on the list
    of hosts passed to the class.

    >>> client = ThriftClient(Cassandra.Client, ['localhost:9160'], retries=2)
    >>> print client.get_string_list_property('keyspaces')
    ['Keyspace1', 'system']
    """
    
    def __init__(self, client_class, hosts, **options):
        self.client_class = client_class
        self.hosts = hosts
        self.options = options

        if not 'retries' in self.options:
            self.options['retries'] = len(self.hosts)

        random.shuffle(self.hosts)
        self.clients = [self.get_client(self.client_class, *server.split(':'))
                            for server in self.hosts if server]

    def get_client(self, clientclass, host, port):
        socket = TSocket.TSocket(host, port)
        transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = clientclass(protocol)
        client.transport = transport
        return client

    # XXX: Needs work
    def next_server(self):
        if not self.clients:
            raise NoServersAvailable
        return self.clients[0]

    def connect(self, client):
        if client.transport.isOpen():
            return True
        else:
            start = time.time()
            while time.time() < start + 10:
                try:
                    client.transport.open()
                except:
                    time.sleep(0.1)
                else:
                    break
            return True
        return False

    def __getattr__(self, attr):
        def callable(*args, **kwargs):
            client = self.next_server()
            if not client:
                raise NoServersAvailable

            attempts = self.options['retries']
            while attempts:
                if self.connect(client):
                    try:
                        return getattr(client, attr)(*args, **kwargs)
                    except (IOError, AttributeError) as E:
                        raise E
                    except TTransport.TTransportException as TE:
                        print TE
                        client = self.next_server()
                        pass
                    else:
                        attempts -= 1
                        pass
        return callable

if __name__ == '__main__':
    import sys
    sys.path.append('gen-py')
    from cassandra import Cassandra
    from cassandra.ttypes import *
    client = ThriftClient(Cassandra.Client, ['localhost:9160', 'aaa:9160'], retries=2)
    print client.get_string_list_property('keyspaces')
