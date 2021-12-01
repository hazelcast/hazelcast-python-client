from hazelcast.core import Address


class TokenProvider(object):
    """TokenProvider is a base class for token providers."""

    def token(self, address=None):
        # type: (TokenProvider, Address) -> bytes
        """Returns a token to be used for token-based authentication.

        Args:
            address (hazelcast.core.Address): Connected address for the member.

        Returns:
            bytes: token as a bytes object.
        """
        pass


class BasicTokenProvider(TokenProvider):
    """BasicTokenProvider sends the given token to the authentication endpoint."""

    def __init__(self, token=""):
        if isinstance(token, str):
            self._token = token.encode("utf-8")
        elif isinstance(token, bytes):
            self._token = token
        else:
            raise TypeError("token must be either a str or bytes object")

    def token(self, address=None):
        return self._token
