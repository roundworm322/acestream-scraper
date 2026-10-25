"""API error mapping for infohash -> content id conversion on add."""
from fastapi.concurrency import run_in_threadpool
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


async def resolve_submitted_infohash(db: Session, infohash: str) -> str:
    """Cache, then engine; raises an APIError the UI can show as-is."""
    resolver = ContentIdResolver.from_settings(db)
    try:
        return await run_in_threadpool(resolver.resolve_one, infohash)
    except (InfohashNotResolvedError, EngineUnavailableError) as exc:
        raise infohash_api_error(exc) from exc
