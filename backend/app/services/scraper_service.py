"""
Service for managing scraping operations
"""
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Tuple, Dict, Any, Optional
import logging

from app.models.models import ScrapedURL, AcestreamChannel
from app.models.url_types import create_url_object
from app.schemas.scraper import ChannelResult, URLResponse, URLCreate, URLUpdate
from app.scrapers import create_scraper_for_url
from app.services.content_id_resolver import (
    ID_KIND_KEY, ContentIdResolver, ResolutionOutcome, is_infohash_record,
)
from app.services.m3u_service import M3UService
from app.services.epg_service import EPGService
from app.services.tvchannel_service import TVChannelService
from app.repositories.channel_repository import ChannelRepository
from app.repositories.url_repository import URLRepository

logger = logging.getLogger(__name__)


class ScraperService:
    """Service for managing scraping operations"""

    def __init__(self, db: Session):
        self.db = db
        self.url_repository = URLRepository(db)

    async def scrape_url(self, url: str, url_type: str = 'auto') -> Tuple[List[ChannelResult], str]:
        """
        Scrape a URL for acestream channels

        Args:
            url: The URL to scrape
            url_type: The URL type ('auto', 'regular', 'zeronet', 'ipfs')

        Returns:
            Tuple[List[ChannelResult], str]: Tuple of (channels, status)
        """
        logger.info(f"Scraping URL: {url} (type: {url_type})")

        custom_recipe = False
        status_changed = False
        try:
            scraper = create_scraper_for_url(url, url_type)
            try:
                source_url = scraper.url_obj.get_normalized_url()
            except AttributeError:
                source_url = create_url_object(url, url_type).get_normalized_url()
            # Inject db, epg_service, tv_channel_service for robust TV channel creation/association
            epg_service = EPGService(self.db)
            tv_channel_service = TVChannelService(self.db)
            scraper.db = self.db
            scraper.epg_service = epg_service
            scraper.tv_channel_service = tv_channel_service
            url_record = self.url_repository.get_by_url(url) or self.url_repository.get_by_url(source_url)
            scraper.scrape_bare_ids = bool(url_record.scrape_bare_ids) if url_record else False
            if url_record and url_record.extraction_recipe:
                custom_recipe = True
                from starlette.concurrency import run_in_threadpool
                from app.schemas.extraction import ExtractionRecipe
                from app.services.extraction_service import fetch_recipe_sample, preview_recipe
                recipe = ExtractionRecipe.model_validate(url_record.extraction_recipe)
                sample = await fetch_recipe_sample(url, url_type)
                preview = await run_in_threadpool(preview_recipe, recipe, sample)
                if not preview.channels or preview.invalid_count:
                    raise ValueError('Recipe returned missing or invalid records; source channels were preserved. Test the recipe before scraping again.')
                raw_channels = [(item.channel_id, item.name, item.metadata) for item in preview.channels]
                status = "OK"
            else:
                raw_channels, status = await scraper.scrape()

            raw_channels, conversion = await self._convert_infohashes(raw_channels)
            if conversion is not None:
                failure = self._conversion_failure(conversion, raw_channels)
                if failure:
                    # Nothing was converted and the engine is the reason: treat
                    # it as a failed scrape so the source keeps its channels.
                    self.db.rollback()
                    await scraper.update_url_status(source_url, failure)
                    return [], failure
                if conversion.unresolved and not status.lower().startswith('error'):
                    status = self._conversion_status(len(raw_channels), conversion)
                    status_changed = True

            # Auto-create TV channels from EPG and assign tv_channel_id
            m3u_service = M3UService()
            raw_channels = m3u_service.auto_create_tv_channels_from_epg(self.db, raw_channels, epg_service, tv_channel_service)

            new_channel_ids = {channel_id for channel_id, _, _ in raw_channels}
            source_urls = {url, source_url}
            source_query = self.db.query(AcestreamChannel).filter(AcestreamChannel.source_url.in_(source_urls))
            # Entries skipped only because the engine was unreachable (or
            # unconfigured) may already be stored under their content id:
            # pruning now would delete them for an outage.
            keep_unlisted = conversion is not None and not conversion.only_definitive_failures
            if new_channel_ids and not keep_unlisted and not (url_record and url_record.extraction_recipe):
                source_query = source_query.filter(~AcestreamChannel.id.in_(new_channel_ids))
                source_query.delete(synchronize_session=False)

            # Persist channels to DB using transaction-scoped upserts
            channel_repository = ChannelRepository(self.db)
            persisted_channels = []
            for channel_id, name, metadata in raw_channels:
                persisted = channel_repository.create_or_update_channel(
                    channel_id=channel_id,
                    name=name,
                    source_url=source_url,
                    group=metadata.get('group_title', metadata.get('group', '')),
                    logo=metadata.get('tvg_logo', metadata.get('logo', '')),
                    tvg_id=metadata.get('tvg_id', ''),
                    tvg_name=metadata.get('tvg_name', ''),
                    # auto_create_tv_channels_from_epg also handles IDs that
                    # have not been persisted yet. Carry its staged
                    # association into the INSERT; existing rows are assigned
                    # by the repository's conflict-checked UPDATE above.
                    tv_channel_id=metadata.get('tv_channel_id'),
                    # Set only for streams addressed by URL instead of by an
                    # acestream id. See M3UService._looks_like_stream.
                    stream_url=metadata.get('stream_url'),
                    stream_headers=metadata.get('stream_headers'),
                    # Catalogue presence is not evidence of a live signal.
                    is_online=None,
                    commit=False,
                )
                persisted_channels.append(persisted)
            self.db.commit()
            if custom_recipe or status_changed:
                await scraper.update_url_status(source_url, status)
            if not status.lower().startswith('error'):
                from starlette.concurrency import run_in_threadpool
                from app.services.epg_matching_automation import EPGMatchingAutomation
                await run_in_threadpool(EPGMatchingAutomation(self.db).run)


            return [
                ChannelResult(
                    channel_id=ch.id,
                    name=ch.name,
                    metadata={
                        'group': ch.group,
                        'logo': ch.logo,
                        'tvg_id': ch.tvg_id,
                        'tvg_name': ch.tvg_name,
                        'source_url': ch.source_url,
                        'tv_channel_id': getattr(ch, 'tv_channel_id', None)
                    }
                ) for ch in persisted_channels
            ], status

        except Exception as e:
            self.db.rollback()
            if custom_recipe:
                await scraper.update_url_status(source_url, "Error: Recipe extraction failed; test the source configuration")
            logger.error(f"Error scraping URL {url}: {str(e)}")
            return [], f"Error: {str(e)}"

    async def _convert_infohashes(
        self, raw_channels: List[Tuple[str, str, Dict[str, Any]]]
    ) -> Tuple[List[Tuple[str, str, Dict[str, Any]]], Optional[ResolutionOutcome]]:
        """Replace infohash-tagged records by their content ids.

        Unresolved records are dropped; duplicates created by the conversion
        (two hashes for one content id, or a hash for an id the source also
        lists directly) keep their first occurrence.
        """
        infohashes = [channel_id for channel_id, _, metadata in raw_channels if is_infohash_record(metadata)]
        if not infohashes:
            return raw_channels, None
        outcome = await ContentIdResolver.from_settings(self.db).resolve_many(infohashes)
        converted = []
        seen = set()
        for channel_id, name, metadata in raw_channels:
            if is_infohash_record(metadata):
                content_id = outcome.resolved.get(channel_id.lower())
                if not content_id:
                    continue
                metadata = {key: value for key, value in metadata.items() if key != ID_KIND_KEY}
                channel_id = content_id
            if channel_id in seen:
                continue
            seen.add(channel_id)
            converted.append((channel_id, name, metadata))
        logger.info(
            "Converted infohashes to content ids: %s resolved (%s engine lookups), %s unknown, %s engine unreachable, %s no engine",
            len(outcome.resolved), outcome.engine_calls, len(outcome.unknown),
            len(outcome.unavailable), len(outcome.no_engine),
        )
        return converted, outcome

    @staticmethod
    def _conversion_failure(outcome: ResolutionOutcome, channels: list) -> Optional[str]:
        if outcome.resolved:
            return None
        if outcome.unavailable:
            return (f"Error: AceStream engine unreachable; {outcome.unresolved} infohash links could not be "
                    "converted to content IDs. Existing channels were kept.")
        if outcome.no_engine and not channels:
            return (f"Error: {outcome.unresolved} infohash links need an AceStream engine to be converted "
                    "to content IDs; configure the engine URL in Settings.")
        return None

    @staticmethod
    def _conversion_status(imported: int, outcome: ResolutionOutcome) -> str:
        reasons = []
        if outcome.unknown:
            reasons.append(f"{len(outcome.unknown)} unknown to the engine")
        if outcome.unavailable:
            reasons.append(f"{len(outcome.unavailable)} engine unreachable")
        if outcome.no_engine:
            reasons.append(f"{len(outcome.no_engine)} no engine configured")
        noun = "infohash" if outcome.unresolved == 1 else "infohashes"
        return f"OK — {imported} imported, {outcome.unresolved} {noun} unresolved ({', '.join(reasons)})"

    def get_scraped_urls(self, skip: int = 0, limit: int = 100) -> List[URLResponse]:
        """Get list of URLs that have been scraped, including channels_found count"""
        urls = self.url_repository.get_all(skip=skip, limit=limit)
        # Get all source_urls in one query for efficiency
        url_to_count = {}
        if urls:
            normalized_url_map = {}
            for url in urls:
                try:
                    normalized_url_map[url.url] = create_url_object(url.url, url.url_type).get_normalized_url()
                except Exception:
                    normalized_url_map[url.url] = url.url
            url_list = list({*normalized_url_map.keys(), *normalized_url_map.values()})
            counts = (
                self.db.query(AcestreamChannel.source_url, func.count(AcestreamChannel.id))
                .filter(AcestreamChannel.source_url.in_(url_list))
                .group_by(AcestreamChannel.source_url)
                .all()
            )
            raw_counts = {url: count for url, count in counts}
            url_to_count = {}
            for original_url, normalized_url in normalized_url_map.items():
                count_keys = {original_url, normalized_url}
                url_to_count[original_url] = sum(raw_counts.get(key, 0) for key in count_keys)
        responses = []
        for url in urls:
            channels_found = url_to_count.get(url.url, 0)
            data = url.__dict__.copy()
            data['channels_found'] = channels_found
            responses.append(URLResponse(**data))
        return responses

    def get_scraped_url(self, url_id: int) -> Optional[URLResponse]:
        """Get a specific scraped URL by ID"""
        url = self.url_repository.get_by_id(url_id)
        return URLResponse.model_validate(url) if url else None

    def get_scraped_url_entity(self, url_id: int) -> Optional[ScrapedURL]:
        """Get raw scraped URL model for service-internal decisions."""
        return self.url_repository.get_by_id(url_id)

    def create_scraped_url(self, url_data: URLCreate) -> URLResponse:
        """Create a new scraped URL or update existing if duplicate"""
        # Check for existing URL
        url = self.url_repository.get_by_url(url_data.url)
        if url:
            # Update fields if duplicate
            url.url_type = url_data.url_type
            url.enabled = url_data.enabled
            url.status = url_data.status
            url.scrape_bare_ids = url_data.scrape_bare_ids
            if "extraction_recipe" in url_data.model_fields_set:
                url.extraction_recipe = url_data.extraction_recipe.model_dump() if url_data.extraction_recipe else None
            url = self.url_repository.update(url)
            return URLResponse.model_validate(url)
        # Create new if not exists
        url = ScrapedURL(
            url=url_data.url,
            url_type=url_data.url_type,
            enabled=url_data.enabled,
            status=url_data.status,
            scrape_bare_ids=url_data.scrape_bare_ids,
            extraction_recipe=url_data.extraction_recipe.model_dump() if url_data.extraction_recipe else None
        )
        self.url_repository.add(url)
        self.db.refresh(url)
        return URLResponse.model_validate(url)

    def update_scraped_url(self, url_id: int, url_data: URLUpdate) -> Optional[URLResponse]:
        """Update a scraped URL"""
        url = self.url_repository.get_by_id(url_id)
        if not url:
            return None

        update_data = url_data.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(url, field, value)

        url = self.url_repository.update(url)
        self.db.refresh(url)
        return URLResponse.model_validate(url)

    def delete_scraped_url(self, url_id: int) -> bool:
        """Delete a scraped URL"""
        url = self.url_repository.get_by_id(url_id)
        if not url:
            return False

        self.url_repository.delete(url)
        return True

    def get_enabled_urls(self) -> List[ScrapedURL]:
        """Get all enabled URLs"""
        return self.url_repository.get_enabled()
