"""API error mapping for infohash -> content id conversion on manual add."""
from sqlalchemy.orm import Session

from app.api.error_handlers import APIError
from app.services.content_id_resolver import (
    ContentIdResolver, EngineNotConfiguredError, InfohashNotResolvedError, describe_failure,
)
from app.services.engine_client import EngineUnavailableError


def infohash_api_error(exc: Exception) -> APIError:
    if isinstance(exc, InfohashNotResolvedError):
        return APIError("INFOHASH_UNRESOLVED", describe_failure(exc), status_code=422)
    if isinstance(exc, EngineNotConfiguredError):
        return APIError("ENGINE_NOT_CONFIGURED", describe_failure(exc), status_code=409)
    return APIError("ENGINE_UNAVAILABLE", describe_failure(exc), status_code=502)


def resolve_submitted_infohash(db: Session, infohash: str) -> str:
    """Cache, then engine; blocking, so only call it from a sync endpoint.

    Raises an APIError the UI can show as-is.
    """
    try:
        return ContentIdResolver.from_settings(db).resolve_one(infohash)
    except (InfohashNotResolvedError, EngineUnavailableError) as exc:
        raise infohash_api_error(exc) from exc
