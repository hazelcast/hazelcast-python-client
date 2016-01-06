LIFECYCLE_STATE_STARTING = 0
LIFECYCLE_STATE_CONNECTED = 1
LIFECYCLE_STATE_DISCONNECTED = 2
LIFECYCLE_STATE_SHUTTING_DOWN = 3
LIFECYCLE_STATE_TERMINATED = 4


class LifecycleService(object):
    def __init__(self):
        self._listeners = []

    def add_listener(self, on_lifecycle_change):
        self._listeners.append(on_lifecycle_change)

    def fire_lifecycle_event(self, new_state):
        for listener in self._listeners:
            listener(new_state)
