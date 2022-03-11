import typing

from hazelcast.core import Address


class TokenProvider:
    """TokenProvider is a base class for token providers."""

    def token(self, address: Address = None) -> bytes:
        """Returns a token to be used for token-based authentication.

        Args:
            address: Connected address for the member.

        Returns:
            token as a bytes object.
        """
        pass


class BasicTokenProvider(TokenProvider):
    """BasicTokenProvider sends the given token to the authentication endpoint."""

    def __init__(self, token: typing.Union[str, bytes] = ""):
        if isinstance(token, str):
            self._token = token.encode("utf-8")
        elif isinstance(token, bytes):
            self._token = token
        else:
            raise TypeError("token must be either a str or bytes object")

    def token(self, address: Address = None) -> bytes:
        return self._token
