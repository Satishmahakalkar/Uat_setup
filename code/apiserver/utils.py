import datetime
from decimal import Decimal
from enum import Enum
from typing import List, Optional, Tuple
import jwt
from starlette.authentication import AuthCredentials, AuthenticationBackend, AuthenticationError, SimpleUser
from starlette.requests import HTTPConnection

import settings


def serialize(values: List[dict]):
    _values = []
    for dict_value in values:
        _dict_value = {}
        for key, value in dict_value.items():
            if isinstance(value, Decimal):
                value = float(value)
            elif isinstance(value, (datetime.date, datetime.datetime)):
                value = str(value)
            elif isinstance(value, Enum):
                value = value.value
            _dict_value[key] = value
        _values.append(_dict_value)
    return _values


class JWTAuthBackend(AuthenticationBackend):

    async def authenticate(self, conn: HTTPConnection) -> Optional[Tuple[AuthCredentials, SimpleUser]]:
        if "Authorization" not in conn.headers:
            return
        auth = conn.headers["Authorization"]
        try:
            scheme, credentials = auth.split()
            if scheme.casefold() != 'Bearer'.casefold():
                return
            decoded = jwt.decode(credentials, settings.SECRET, algorithms=["HS256"])
        except (jwt.exceptions.InvalidTokenError, ValueError) as exc:
            raise AuthenticationError('Invalid JWT')
        return AuthCredentials(["authenticated"]), SimpleUser("admin")