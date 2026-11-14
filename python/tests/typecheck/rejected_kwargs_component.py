"""Pyright must reject a ParamSpec kwargs component used without args."""

from collections.abc import Awaitable, Callable
from typing import ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")


def send(actor: Callable[P, R], **kwargs: P.kwargs) -> Awaitable[str]: ...
