# Defines the core permissions settings of the system
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, List


class UnauthorisedError(Exception):
    """
    Exception raised if a user is unauthorised to perform an access
    """
    pass


class Action(Enum):
    """
    Used within the context of a Permission to define whether a user of
    a given Principal has access to a Resource
    Can be subclassed to provide additional action types
    """
    ALLOW = auto()
    DENY = auto()


class BaseAccessType(Enum):
    """
    Base type for accesses.
    """
    ...


@dataclass(frozen=True)
class Principal:
    """
    Encodes the groups or roles of a given user
    """
    method: Any
    value: Any


@dataclass(frozen=True)
class Permission:
    """
    Used within the context of an access to allow/deny access to users of a
    given principal
    """
    action: Action
    principal: Principal


def has_permission(principals: List[Principal],
                   permissions: List[Permission]) -> Action:
    """
    Returns the Action that a user with _principals_ can perform on an
    access with _permissions_
    """
    for permission in permissions:
        if permission.principal in principals:
            return permission.action
    return Action.DENY
