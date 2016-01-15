import logging
import uuid

LIFECYCLE_STATE_STARTING = "starting"
LIFECYCLE_STATE_CONNECTED = "connected"
LIFECYCLE_STATE_DISCONNECTED = "disconnected"
LIFECYCLE_STATE_SHUTTING_DOWN = "shutting_down"
LIFECYCLE_STATE_SHUTDOWN = "shutdown"


class LifecycleService(object):
    logger = logging.getLogger("LifecycleService")

    def __init__(self, config):
        self._listeners = {}

        for listener in config.lifecycle_listeners:
            self.add_listener(listener)

        self.is_live = True
        self.fire_lifecycle_event(LIFECYCLE_STATE_STARTING)

    def add_listener(self, on_lifecycle_change):
        id = str(uuid.uuid4())
        self._listeners[id] = on_lifecycle_change
        return id

    def remove_listener(self, registration_id):
        try:
            self._listeners.pop(registration_id)
            return True
        except KeyError:
            return False

    def fire_lifecycle_event(self, new_state):
        if new_state == LIFECYCLE_STATE_SHUTTING_DOWN:
            self.is_live = False

        self.logger.debug("New Lifecycle state is %s", new_state)
        for listener in self._listeners.values():
            try:
                listener(new_state)
            except:
                self.logger.exception("Exception in lifecycle listener")
