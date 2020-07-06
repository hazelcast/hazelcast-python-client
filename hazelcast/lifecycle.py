import logging
import uuid

from hazelcast.util import create_git_info

LIFECYCLE_STATE_STARTING = "STARTING"
"""Fired when the member is starting."""

LIFECYCLE_STATE_STARTED = "STARTED"
"""Fired when the member start is completed."""

LIFECYCLE_STATE_SHUTTING_DOWN = "SHUTTING_DOWN"
"""Fired when the member is shutting down."""

LIFECYCLE_STATE_SHUTDOWN = "SHUTDOWN"
"""Fired when the member is shut down is completed."""

LIFECYCLE_STATE_CLIENT_CONNECTED = "CONNECTED"
"""Fired when a client is connected to the member."""

LIFECYCLE_STATE_CLIENT_DISCONNECTED = "DISCONNECTED"
"""Fired when a client is disconnected from the member."""

LIFECYCLE_STATE_CLIENT_CHANGED_CLUSTER = "CLIENT_CHANGED_CLUSTER"
"""Fired when a client is connected to a new cluster."""


class LifecycleService(object):
    """
    LifecycleService allows you to shutdown, terminate, and listen to LifecycleEvent's on HazelcastInstances.
    """
    logger = logging.getLogger("HazelcastClient.LifecycleService")
    state = None

    def __init__(self, client, config, logger_extras=None):
        self._listeners = {}
        self._client = client
        self._logger_extras = logger_extras

        for listener in config.lifecycle_listeners:
            self.add_listener(listener)

        self._git_info = create_git_info()
        self.is_live = False
        # self.fire_lifecycle_event(LIFECYCLE_STATE_STARTING)

    def add_listener(self, on_lifecycle_change):
        """
        Add a listener object to listen for lifecycle events.

        :param on_lifecycle_change: (Function), function to be called when LifeCycle state is changed.
        :return: (str), id of the listener.
        """
        id = str(uuid.uuid4())
        self._listeners[id] = on_lifecycle_change
        return id

    def remove_listener(self, registration_id):
        """
        Removes a lifecycle listener.

        :param registration_id: (str), the id of the listener to be removed.
        :return: (bool), ``true`` if the listener is removed successfully, ``false`` otherwise.
        """
        try:
            self._listeners.pop(registration_id)
            return True
        except KeyError:
            return False

    def fire_lifecycle_event(self, new_state):
        """
        Called when instance's state changes.

        :param new_state: (Lifecycle State), the new state of the instance.
        """

        self.state = new_state
        self.logger.info(self._git_info + "HazelcastClient is %s", new_state, extra=self._logger_extras)
        for listener in list(self._listeners.values()):
            try:
                listener(new_state)
            except:
                self.logger.exception("Exception in lifecycle listener", extra=self._logger_extras)

    def start(self):
        self.fire_lifecycle_event(LIFECYCLE_STATE_STARTING)
        self.is_live = True
        self.fire_lifecycle_event(LIFECYCLE_STATE_STARTED)

    def shutdown(self):
        if not self.is_live:
            return

        self.is_live = False

        self.fire_lifecycle_event(LIFECYCLE_STATE_SHUTTING_DOWN)
        self._client.do_shutdown()
        self.fire_lifecycle_event(LIFECYCLE_STATE_SHUTDOWN)
