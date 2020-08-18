import logging
import uuid

from hazelcast import six
from hazelcast.util import create_git_info, enum

LIFECYCLE_STATE_STARTING = "STARTING"
LIFECYCLE_STATE_CONNECTED = "CONNECTED"
LIFECYCLE_STATE_DISCONNECTED = "DISCONNECTED"
LIFECYCLE_STATE_SHUTTING_DOWN = "SHUTTING_DOWN"
LIFECYCLE_STATE_SHUTDOWN = "SHUTDOWN"

LifecycleState = enum(
    STARTING="STARTING",
    STARTED="STARTED",
    SHUTTING_DOWN="SHUTTING_DOWN",
    SHUTDOWN="SHUTDOWN",
    CONNECTED="CONNECTED",
    DISCONNECTED="DISCONNECTED",
)


logger = logging.getLogger(__name__)


class LifecycleService(object):
    """
    LifecycleService allows you to shutdown, terminate, and listen to LifecycleEvent's on HazelcastInstances.
    """

    def __init__(self, client, logger_extras):
        self.live = True
        self._listeners = {}
        self._client = client
        self._logger_extras = logger_extras

        for listener in self._client.config.lifecycle_listeners:
            self.add_listener(listener)

        self._git_info = create_git_info()

    def add_listener(self, on_state_change):
        """
        Add a listener object to listen for lifecycle events.

        :param on_state_change: (Function), function to be called when LifeCycle state is changed.
        :return: (str), id of the listener.
        """
        listener_id = str(uuid.uuid4())
        self._listeners[listener_id] = on_state_change
        return listener_id

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
        logger.info(self._git_info + "HazelcastClient is %s", new_state, extra=self._logger_extras)
        for on_state_change in six.itervalues(self._listeners):
            if on_state_change:
                try:
                    on_state_change(new_state)
                except:
                    logger.exception("Exception in lifecycle listener", extra=self._logger_extras)

    def start(self):
        if self.live:
            return

        self.fire_lifecycle_event(LifecycleState.STARTING)
        self.live = True
        self.fire_lifecycle_event(LifecycleState.STARTED)

    def shutdown(self):
        if not self.live:
            return

        self.live = False
        self.fire_lifecycle_event(LifecycleState.SHUTTING_DOWN)
        self._client.do_shutdown()
        self.fire_lifecycle_event(LifecycleState.SHUTDOWN)

