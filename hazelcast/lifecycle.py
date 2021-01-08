import logging
import uuid

from hazelcast import six, __version__

_logger = logging.getLogger(__name__)


class LifecycleState(object):
    """Lifecycle states."""

    STARTING = "STARTING"
    """
    The client is starting.
    """

    STARTED = "STARTED"
    """
    The client has started.
    """

    CONNECTED = "CONNECTED"
    """
    The client connected to a member.
    """

    SHUTTING_DOWN = "SHUTTING_DOWN"
    """
    The client is shutting down.
    """

    DISCONNECTED = "DISCONNECTED"
    """
    The client disconnected from a member.
    """

    SHUTDOWN = "SHUTDOWN"
    """
    The client has shutdown.
    """


class LifecycleService(object):
    """
    Lifecycle service for the Hazelcast client. Allows to determine
    state of the client and add or remove lifecycle listeners.
    """

    def __init__(self, internal_lifecycle_service):
        self._service = internal_lifecycle_service

    def is_running(self):
        """
        Checks whether or not the instance is running.

        Returns:
            bool: ``True`` if the client is active and running, ``False`` otherwise.
        """
        return self._service.running

    def add_listener(self, on_state_change):
        """
        Adds a listener to listen for lifecycle events.

        Args:
            on_state_change (function): Function to be called when lifecycle state is changed.

        Returns:
            str: Registration id of the listener
        """
        return self._service.add_listener(on_state_change)

    def remove_listener(self, registration_id):
        """
        Removes a lifecycle listener.

        Args:
            registration_id (str): The id of the listener to be removed.

        Returns:
            bool: ``True`` if the listener is removed successfully, ``False`` otherwise.
        """
        self._service.remove_listener(registration_id)


class _InternalLifecycleService(object):
    def __init__(self, config):
        self.running = False
        self._listeners = {}

        lifecycle_listeners = config.lifecycle_listeners
        if lifecycle_listeners:
            for listener in lifecycle_listeners:
                self.add_listener(listener)

    def start(self):
        if self.running:
            return

        self.fire_lifecycle_event(LifecycleState.STARTING)
        self.running = True
        self.fire_lifecycle_event(LifecycleState.STARTED)

    def shutdown(self):
        self.running = False

    def add_listener(self, on_state_change):
        listener_id = str(uuid.uuid4())
        self._listeners[listener_id] = on_state_change
        return listener_id

    def remove_listener(self, registration_id):
        try:
            self._listeners.pop(registration_id)
            return True
        except KeyError:
            return False

    def fire_lifecycle_event(self, new_state):
        """Called when instance's state changes.

        Args:
            new_state (str): The new state of the instance.
        """
        _logger.info("HazelcastClient %s is %s", __version__, new_state)
        for on_state_change in six.itervalues(self._listeners):
            if on_state_change:
                try:
                    on_state_change(new_state)
                except:
                    _logger.exception("Exception in lifecycle listener")
