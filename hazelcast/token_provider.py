from six import string_types


class TokenProvider(object):
    """TokenProvider is a base class for token providers."""

    def token(self):
        # type: (TokenProvider) -> bytes
        """Returns a token to be used for token-based authentication.

        Returns:
            bytes: token as a bytes object.
        """
        pass


class BasicTokenProvider(TokenProvider):
    """BasicTokenProvider sends the given token to the authentication endpoint."""

    def __init__(self, token=""):
        if isinstance(token, string_types):
            self._token = token.encode("utf-8")
        elif isinstance(token, bytes):
            self._token = token
        else:
            raise ValueError("token must be either a str or bytes object")

    def token(self):
        # type: (BasicTokenProvider) -> bytes
        """Returns a token to be used for token-based authentication.

        Returns:
            bytes: token as a bytes object.
        """
        return self._token
