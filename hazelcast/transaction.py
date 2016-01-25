import logging
import threading
import time
from hazelcast.exception import HazelcastInstanceNotActiveError, TransactionError
from hazelcast.future import make_blocking
from hazelcast.protocol.codec import transaction_create_codec, transaction_commit_codec, transaction_rollback_codec
from hazelcast.proxy.transactional_list import TransactionalList
from hazelcast.proxy.transactional_map import TransactionalMap
from hazelcast.proxy.transactional_multi_map import TransactionalMultiMap
from hazelcast.proxy.transactional_queue import TransactionalQueue
from hazelcast.proxy.transactional_set import TransactionalSet
from hazelcast.util import thread_id

_STATE_ACTIVE = "active"
_STATE_NOT_STARTED = "not_started"
_STATE_COMMITTED = "committed"
_STATE_ROLLED_BACK = "rolled_back"
_STATE_PARTIAL_COMMIT = "rolling_back"

"""
The two phase commit is separated in 2 parts. First it tries to execute the prepare; if there are any conflicts,
the prepare will fail. Once the prepare has succeeded, the commit (writing the changes) can be executed.

Hazelcast also provides three phase transaction by automatically copying the backlog to another member so that in case
of failure during a commit, another member can continue the commit from backup.
"""
TWO_PHASE = 1

"""
The one phase transaction executes a transaction using a single step at the end; committing the changes. There
is no prepare of the transactions, so conflicts are not detected. If there is a conflict, then when the transaction
commits the changes, some of the changes are written and others are not; leaving the system in a potentially permanent
inconsistent state.
"""
ONE_PHASE = 2

RETRY_COUNT = 20


class TransactionManager(object):
    logger = logging.getLogger("TransactionManager")

    def __init__(self, client):
        self._client = client

    def _connect(self):
        for count in xrange(0, RETRY_COUNT):
            try:
                address = self._client.load_balancer.next_address()
                return self._client.connection_manager.get_or_connect(address).result()
            except (IOError, HazelcastInstanceNotActiveError):
                self.logger.debug("Could not get a connection for the transaction. Attempt %d of %d", count,
                                  RETRY_COUNT,
                                  exc_info=True)
                if count + 1 == RETRY_COUNT:
                    raise

    def new_transaction(self, timeout, durability, transaction_type):
        connection = self._connect()
        return Transaction(self._client, connection, timeout, durability, transaction_type)


class Transaction(object):
    state = _STATE_NOT_STARTED
    id = None
    start_time = None
    _locals = threading.local()
    logger = logging.getLogger("Transaction")
    thread_id = None

    def __init__(self, client, connection, timeout, durability, transaction_type):
        self.connection = connection
        self.timeout = timeout
        self.durability = durability
        self.transaction_type = transaction_type
        self.client = client
        self._objects = {}

    def begin(self):
        if hasattr(self._locals, 'transaction_exists') and self._locals.transaction_exists:
            raise TransactionError("Nested transactions are not allowed.")
        if self.state != _STATE_NOT_STARTED:
            raise TransactionError("Transaction has already been started.")
        self._locals.transaction_exists = True
        self.start_time = time.time()
        self.thread_id = thread_id()
        try:
            request = transaction_create_codec.encode_request(timeout=self.timeout * 1000, durability=self.durability,
                                                              transaction_type=self.transaction_type,
                                                              thread_id=self.thread_id)
            response = self.client.invoker.invoke_on_connection(request, self.connection).result()
            self.id = transaction_create_codec.decode_response(response)["response"]
            self.state = _STATE_ACTIVE
        except:
            self._locals.transaction_exists = False
            raise

    def commit(self):
        self._check_thread()
        if self.state != _STATE_ACTIVE:
            raise TransactionError("Transaction is not active.")
        try:
            self._check_timeout()
            request = transaction_commit_codec.encode_request(self.id, self.thread_id)
            self.client.invoker.invoke_on_connection(request, self.connection).result()
            self.state = _STATE_COMMITTED
        except:
            self.state = _STATE_PARTIAL_COMMIT
            raise
        finally:
            self._locals.transaction_exists = False

    def rollback(self):
        self._check_thread()
        if self.state not in (_STATE_ACTIVE, _STATE_PARTIAL_COMMIT):
            raise TransactionError("Transaction is not active.")
        try:
            if self.state != _STATE_PARTIAL_COMMIT:
                request = transaction_rollback_codec.encode_request(self.id, self.thread_id)
                self.client.invoker.invoke_on_connection(request, self.connection).result()
            self.state = _STATE_ROLLED_BACK
        finally:
            self._locals.transaction_exists = False

    def get_list(self, name):
        return self._get_or_create_object(name, TransactionalList)

    def get_map(self, name):
        return self._get_or_create_object(name, TransactionalMap)

    def get_multi_map(self, name):
        return self._get_or_create_object(name, TransactionalMultiMap)

    def get_queue(self, name):
        return self._get_or_create_object(name, TransactionalQueue)

    def get_set(self, name):
        return self._get_or_create_object(name, TransactionalSet)

    def _get_or_create_object(self, name, proxy_type):
        if self.state != _STATE_ACTIVE:
            raise TransactionError("Transaction is not in active state.")
        self._check_thread()
        key = (proxy_type, name)
        try:
            return self._objects[key]
        except KeyError:
            proxy = proxy_type(name, self)
            self._objects[key] = proxy
            return make_blocking(proxy)

    def _check_thread(self):
        if not thread_id() == self.thread_id:
            raise TransactionError("Transaction cannot span multiple threads.")

    def _check_timeout(self):
        if time.time() > self.timeout + self.start_time:
            raise TransactionError("Transaction has timed out.")

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, type, value, traceback):
        if not type and not value and self.state == _STATE_ACTIVE:
            self.commit()
        elif self.state in (_STATE_PARTIAL_COMMIT, _STATE_ACTIVE):
            self.rollback()
