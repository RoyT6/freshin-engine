#!/usr/bin/env python3
"""
SCHIG - Scared Child Gatherer
==============================
Nightly Data Collection System for FrameCore BFD ViewerDBX

Collects fresh streaming/entertainment data from multiple APIs:
- TMDB (trending, popular, upcoming movies/TV)
- FlixPatrol (platform rankings)
- Streaming Availability (platform catalog changes)
- IMDB (ratings, metadata)
- Rotten Tomatoes (critic/audience scores)
- HBO Max, Disney+ (platform-specific data)
- Seeking Alpha, Reuters (financial/business news)
- Twitter Trends (social sentiment)
- Sports Highlights (competing content)

Version: 1.0.0
Created: 2026-01-13
"""

import asyncio
import aiohttp
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
import pandas as pd
from requests.auth import HTTPBasicAuth
import requests
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import xml.etree.ElementTree as ET

# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class APIConfig:
    """API configuration container"""
    # RapidAPI Universal Key
    RAPIDAPI_KEY: str = "2e5f1ffd8dmsh65f83d24ed46b1cp18bd77jsn16c55b9c022f"

    # FlixPatrol
    FLIXPATROL_KEY: str = "aku_4bXKmMWPSCaKxwn2tVzTXmcg"
    FLIXPATROL_BASE: str = "https://api.flixpatrol.com/v2"

    # OMDB
    OMDB_KEY: str = "2d8b9d62"
    OMDB_BASE: str = "http://www.omdbapi.com"

    # TMDB
    TMDB_KEY: str = "8fe4d782052d6c6dce4cf23f4b01b3c5"
    TMDB_BEARER: str = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI4ZmU0ZDc4MjA1MmQ2YzZkY2U0Y2YyM2Y0YjAxYjNjNSIsIm5iZiI6MTc1OTk3NzE1Ny4zMTEsInN1YiI6IjY4ZTcxZWM1MjdjZjcxOTcyNDczZDc5NiIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ._Q_5lA7npuYpFJzYOFAsvxdGjzzKZSoCOt-63BJ8m4U"
    TMDB_BASE: str = "https://api.themoviedb.org/3"

    # TheTVDB
    TVDB_KEY: str = "03fb627f-e08c-418c-a1b2-3b40869def52"
    TVDB_BASE: str = "https://api4.thetvdb.com/v4"

    # Output paths
    OUTPUT_DIR: str = r"C:\Users\RoyT6\Downloads\FreshIn"
    LOG_FILE: str = r"C:\Users\RoyT6\Downloads\FreshIn\collection_log.txt"


# RapidAPI Hosts
RAPIDAPI_HOSTS = {
    "streaming_availability": "streaming-availability.p.rapidapi.com",
    "imdb": "imdb-api17.p.rapidapi.com",
    "instagram_stats": "instagram-statistics-api.p.rapidapi.com",
    "twitter_trends": "twitter-trends-api.p.rapidapi.com",
    "seeking_alpha": "seeking-alpha.p.rapidapi.com",
    "reuters": "reuters-business-and-financial-news.p.rapidapi.com",
    "meteostat": "meteostat.p.rapidapi.com",
    "sports_highlights": "sport-highlights-api.p.rapidapi.com",
    "rotten_tomatoes": "rottentomato.p.rapidapi.com",
    "hbo_max": "hbo-max-movies-and-tv-shows-api-by-apirobots.p.rapidapi.com",
    "disney_plus": "disney-plus-top-movies-and-tv-shows-api-by-apirobots.p.rapidapi.com",
    "world_events": "world-historical-events-api.p.rapidapi.com",
    "ai_recommender": "ai-movie-recommender.p.rapidapi.com",
}

# Countries to collect streaming data for
TARGET_COUNTRIES = ["us", "gb", "ca", "au", "de", "fr", "in", "br", "jp", "mx",
                    "es", "it", "nl", "se", "no", "dk", "pl", "tr"]

# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging(config: APIConfig) -> logging.Logger:
    """Configure logging"""
    logger = logging.getLogger("ScaredChildGatherer")
    logger.setLevel(logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # File handler
    file_handler = logging.FileHandler(config.LOG_FILE, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(console_format)
    logger.addHandler(file_handler)

    return logger


# ============================================================================
# BASE COLLECTOR CLASS
# ============================================================================

class BaseCollector:
    """Base class for API collectors with retry and rate limiting"""

    def __init__(self, config: APIConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.session: Optional[aiohttp.ClientSession] = None
        self.results: List[Dict] = []

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def get_rapidapi_headers(self, host: str) -> Dict[str, str]:
        """Get standard RapidAPI headers"""
        return {
            "x-rapidapi-key": self.config.RAPIDAPI_KEY,
            "x-rapidapi-host": host
        }

    @retry(wait=wait_exponential(multiplier=1, min=2, max=60),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)))
    async def fetch_json(self, url: str, headers: Dict = None, params: Dict = None) -> Dict:
        """Fetch JSON with retry logic"""
        try:
            async with self.session.get(url, headers=headers, params=params, timeout=30) as response:
                if response.status == 429:
                    self.logger.warning(f"Rate limited on {url}, retrying...")
                    raise aiohttp.ClientError("Rate limited")
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            self.logger.error(f"Error fetching {url}: {e}")
            raise


# ============================================================================
# TMDB COLLECTOR
# ============================================================================

class TMDBCollector(BaseCollector):
    """Collect data from TMDB API"""

    async def collect(self) -> List[Dict]:
        """Collect all TMDB data"""
        self.logger.info("Starting TMDB collection...")
        results = []

        headers = {
            "Authorization": f"Bearer {self.config.TMDB_BEARER}",
            "accept": "application/json"
        }

        endpoints = [
            ("trending/movie/week", "trending_movies_week"),
            ("trending/tv/week", "trending_tv_week"),
            ("movie/popular", "popular_movies"),
            ("tv/popular", "popular_tv"),
            ("movie/upcoming", "upcoming_movies"),
            ("movie/now_playing", "now_playing_movies"),
            ("tv/on_the_air", "on_the_air_tv"),
            ("tv/airing_today", "airing_today_tv"),
            ("movie/top_rated", "top_rated_movies"),
            ("tv/top_rated", "top_rated_tv"),
        ]

        for endpoint, data_type in endpoints:
            try:
                url = f"{self.config.TMDB_BASE}/{endpoint}"
                data = await self.fetch_json(url, headers=headers)
                results.append({
                    "source": "tmdb",
                    "type": data_type,
                    "data": data
                })
                self.logger.info(f"  TMDB {data_type}: {len(data.get('results', []))} items")
                await asyncio.sleep(0.25)  # Rate limiting
            except Exception as e:
                self.logger.error(f"  TMDB {data_type} failed: {e}")

        # Get genre lists
        for media_type in ["movie", "tv"]:
            try:
                url = f"{self.config.TMDB_BASE}/genre/{media_type}/list"
                data = await self.fetch_json(url, headers=headers)
                results.append({
                    "source": "tmdb",
                    "type": f"{media_type}_genres",
                    "data": data
                })
            except Exception as e:
                self.logger.error(f"  TMDB {media_type}_genres failed: {e}")

        self.logger.info(f"TMDB collection complete: {len(results)} datasets")
        return results


# ============================================================================
# STREAMING AVAILABILITY COLLECTOR
# ============================================================================

class StreamingAvailabilityCollector(BaseCollector):
    """Collect data from Streaming Availability API"""

    async def collect(self) -> List[Dict]:
        """Collect streaming availability data"""
        self.logger.info("Starting Streaming Availability collection...")
        results = []

        headers = self.get_rapidapi_headers(RAPIDAPI_HOSTS["streaming_availability"])

        # Get countries list
        try:
            url = "https://streaming-availability.p.rapidapi.com/countries"
            data = await self.fetch_json(url, headers=headers, params={"output_language": "en"})
            results.append({
                "source": "streaming_availability",
                "type": "countries",
                "data": data
            })
            self.logger.info(f"  Streaming countries: {len(data) if isinstance(data, dict) else 0}")
        except Exception as e:
            self.logger.error(f"  Streaming countries failed: {e}")

        # Get changes for target countries
        for country in TARGET_COUNTRIES[:10]:  # Limit to avoid rate limits
            try:
                url = "https://streaming-availability.p.rapidapi.com/changes"
                params = {
                    "country": country,
                    "change_type": "new",
                    "item_type": "show",
                    "output_language": "en"
                }
                data = await self.fetch_json(url, headers=headers, params=params)
                results.append({
                    "source": "streaming_availability",
                    "country": country,
                    "type": "new_shows",
                    "data": data
                })
                self.logger.info(f"  Streaming {country}: {len(data.get('changes', []))} changes")
                await asyncio.sleep(0.5)  # Rate limiting
            except Exception as e:
                self.logger.error(f"  Streaming {country} failed: {e}")

        # Get genres
        try:
            url = "https://streaming-availability.p.rapidapi.com/genres"
            data = await self.fetch_json(url, headers=headers)
            results.append({
                "source": "streaming_availability",
                "type": "genres",
                "data": data
            })
        except Exception as e:
            self.logger.error(f"  Streaming genres failed: {e}")

        self.logger.info(f"Streaming Availability complete: {len(results)} datasets")
        return results


# ============================================================================
# FLIXPATROL COLLECTOR
# ============================================================================

class FlixPatrolCollector(BaseCollector):
    """Collect data from FlixPatrol API"""

    def collect_sync(self) -> List[Dict]:
        """Collect FlixPatrol data (synchronous due to Basic Auth)"""
        self.logger.info("Starting FlixPatrol collection...")
        results = []

        auth = HTTPBasicAuth(self.config.FLIXPATROL_KEY, "")

        # Get titles
        try:
            url = f"{self.config.FLIXPATROL_BASE}/titles"
            response = requests.get(url, auth=auth, timeout=30)
            response.raise_for_status()
            data = response.json()
            results.append({
                "source": "flixpatrol",
                "endpoint": "titles",
                "data": data
            })
            title_count = len(data.get('data', []))
            self.logger.info(f"  FlixPatrol titles: {title_count} items")
        except Exception as e:
            self.logger.error(f"  FlixPatrol titles failed: {e}")

        # Get top-10 rankings for key platforms
        platforms = ["netflix", "amazon-prime-video", "disney-plus", "hbo-max", "apple-tv-plus"]
        for platform in platforms:
            try:
                url = f"{self.config.FLIXPATROL_BASE}/top10/{platform}"
                response = requests.get(url, auth=auth, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    results.append({
                        "source": "flixpatrol",
                        "endpoint": f"top10_{platform}",
                        "data": data
                    })
                    self.logger.info(f"  FlixPatrol {platform} top-10: OK")
                time.sleep(0.5)
            except Exception as e:
                self.logger.error(f"  FlixPatrol {platform} failed: {e}")

        self.logger.info(f"FlixPatrol collection complete: {len(results)} datasets")
        return results


# ============================================================================
# IMDB COLLECTOR
# ============================================================================

class IMDBCollector(BaseCollector):
    """Collect data from IMDB API via RapidAPI"""

    async def collect(self) -> List[Dict]:
        """Collect IMDB data"""
        self.logger.info("Starting IMDB collection...")
        results = []

        headers = self.get_rapidapi_headers(RAPIDAPI_HOSTS["imdb"])
        base_url = "https://imdb-api17.p.rapidapi.com"

        # Get popular movies
        try:
            url = f"{base_url}/mostPopularMovies"
            params = {"limit": "100"}
            data = await self.fetch_json(url, headers=headers, params=params)
            results.append({
                "source": "imdb",
                "type": "popular_movies",
                "data": data
            })
            self.logger.info(f"  IMDB popular movies: OK")
        except Exception as e:
            self.logger.error(f"  IMDB popular movies failed: {e}")

        # Get popular TV
        try:
            url = f"{base_url}/mostPopularTVs"
            params = {"limit": "100"}
            data = await self.fetch_json(url, headers=headers, params=params)
            results.append({
                "source": "imdb",
                "type": "popular_tv",
                "data": data
            })
            self.logger.info(f"  IMDB popular TV: OK")
        except Exception as e:
            self.logger.error(f"  IMDB popular TV failed: {e}")

        # Get available genres
        try:
            url = f"{base_url}/genres"
            data = await self.fetch_json(url, headers=headers)
            results.append({
                "source": "imdb",
                "type": "genres",
                "data": data
            })
        except Exception as e:
            self.logger.error(f"  IMDB genres failed: {e}")

        # Get available title types
        try:
            url = f"{base_url}/titleTypes"
            data = await self.fetch_json(url, headers=headers)
            results.append({
                "source": "imdb",
                "type": "title_types",
                "data": data
            })
        except Exception as e:
            self.logger.error(f"  IMDB title types failed: {e}")

        self.logger.info(f"IMDB collection complete: {len(results)} datasets")
        return results


# ============================================================================
# ROTTEN TOMATOES COLLECTOR
# ============================================================================

class RottenTomatoesCollector(BaseCollector):
    """Collect data from Rotten Tomatoes API"""

    async def collect(self) -> List[Dict]:
        """Collect Rotten Tomatoes data"""
        self.logger.info("Starting Rotten Tomatoes collection...")
        results = []

        headers = self.get_rapidapi_headers(RAPIDAPI_HOSTS["rotten_tomatoes"])
        base_url = "https://rottentomato.p.rapidapi.com"

        # Get streaming movies by genre
        genres = ["action", "comedy", "drama", "horror", "sci-fi", "thriller"]
        for genre in genres:
            try:
                url = f"{base_url}/streamingmovies"
                params = {"genre": genre, "sortby": "critic_highest"}
                data = await self.fetch_json(url, headers=headers, params=params)
                results.append({
                    "source": "rotten_tomatoes",
                    "type": f"streaming_movies_{genre}",
                    "data": data
                })
                self.logger.info(f"  RT {genre} movies: OK")
                await asyncio.sleep(0.5)
            except Exception as e:
                self.logger.error(f"  RT {genre} movies failed: {e}")

        # Get Netflix top 100
        try:
            url = f"{base_url}/today-top100TVshows-netflix"
            data = await self.fetch_json(url, headers=headers)
            results.append({
                "source": "rotten_tomatoes",
                "type": "netflix_top100_tv",
                "data": data
            })
            self.logger.info(f"  RT Netflix top 100: OK")
        except Exception as e:
            self.logger.error(f"  RT Netflix top 100 failed: {e}")

        self.logger.info(f"Rotten Tomatoes complete: {len(results)} datasets")
        return results


# ============================================================================
# HBO MAX COLLECTOR
# ============================================================================

class HBOMaxCollector(BaseCollector):
    """Collect data from HBO Max API"""

    async def collect(self) -> List[Dict]:
        """Collect HBO Max data"""
        self.logger.info("Starting HBO Max collection...")
        results = []

        headers = self.get_rapidapi_headers(RAPIDAPI_HOSTS["hbo_max"])
        base_url = "https://hbo-max-movies-and-tv-shows-api-by-apirobots.p.rapidapi.com"

        # Get titles with pagination
        for page in range(1, 6):  # First 5 pages
            try:
                url = f"{base_url}/v1/hbo-max"
                params = {"page": str(page)}
                data = await self.fetch_json(url, headers=headers, params=params)
                results.append({
                    "source": "hbo_max",
                    "type": "titles",
                    "page": page,
                    "data": data
                })
                self.logger.info(f"  HBO Max page {page}: OK")
                await asyncio.sleep(0.3)
            except Exception as e:
                self.logger.error(f"  HBO Max page {page} failed: {e}")
                break

        # Get random title
        try:
            url = f"{base_url}/v1/hbo-max/random"
            data = await self.fetch_json(url, headers=headers)
            results.append({
                "source": "hbo_max",
                "type": "random_title",
                "data": data
            })
        except Exception as e:
            self.logger.error(f"  HBO Max random failed: {e}")

        self.logger.info(f"HBO Max complete: {len(results)} datasets")
        return results


# ============================================================================
# DISNEY+ COLLECTOR
# ============================================================================

class DisneyPlusCollector(BaseCollector):
    """Collect data from Disney+ API"""

    async def collect(self) -> List[Dict]:
        """Collect Disney+ data"""
        self.logger.info("Starting Disney+ collection...")
        results = []

        headers = self.get_rapidapi_headers(RAPIDAPI_HOSTS["disney_plus"])
        base_url = "https://disney-plus-top-movies-and-tv-shows-api-by-apirobots.p.rapidapi.com"

        # Get titles with pagination
        for page in range(1, 6):  # First 5 pages
            try:
                url = f"{base_url}/v1/disney-plus-top"
                params = {"page": str(page)}
                data = await self.fetch_json(url, headers=headers, params=params)
                results.append({
                    "source": "disney_plus",
                    "type": "titles",
                    "page": page,
                    "data": data
                })
                self.logger.info(f"  Disney+ page {page}: OK")
                await asyncio.sleep(0.3)
            except Exception as e:
                self.logger.error(f"  Disney+ page {page} failed: {e}")
                break

        # Get random title
        try:
            url = f"{base_url}/v1/disney-plus-top/random"
            data = await self.fetch_json(url, headers=headers)
            results.append({
                "source": "disney_plus",
                "type": "random_title",
                "data": data
            })
        except Exception as e:
            self.logger.error(f"  Disney+ random failed: {e}")

        self.logger.info(f"Disney+ complete: {len(results)} datasets")
        return results


# ============================================================================
# SEEKING ALPHA COLLECTOR (Financial News)
# ============================================================================

class SeekingAlphaCollector(BaseCollector):
    """Collect financial news from Seeking Alpha"""

    async def collect(self) -> List[Dict]:
        """Collect Seeking Alpha data for streaming companies"""
        self.logger.info("Starting Seeking Alpha collection...")
        results = []

        headers = self.get_rapidapi_headers(RAPIDAPI_HOSTS["seeking_alpha"])
        base_url = "https://seeking-alpha.p.rapidapi.com"

        # Streaming company symbols
        symbols = ["NFLX", "DIS", "WBD", "PARA", "CMCSA", "AMZN", "AAPL"]

        for symbol in symbols:
            try:
                # Get news/analysis for each company
                url = f"{base_url}/v2/auto-complete"
                params = {"query": symbol, "type": "symbols", "size": "5"}
                data = await self.fetch_json(url, headers=headers, params=params)
                results.append({
                    "source": "seeking_alpha",
                    "type": "company_search",
                    "symbol": symbol,
                    "data": data
                })
                self.logger.info(f"  Seeking Alpha {symbol}: OK")
                await asyncio.sleep(0.5)
            except Exception as e:
                self.logger.error(f"  Seeking Alpha {symbol} failed: {e}")

        self.logger.info(f"Seeking Alpha complete: {len(results)} datasets")
        return results


# ============================================================================
# TWITTER TRENDS COLLECTOR
# ============================================================================

class TwitterTrendsCollector(BaseCollector):
    """Collect Twitter trends for social sentiment"""

    async def collect(self) -> List[Dict]:
        """Collect Twitter trends data"""
        self.logger.info("Starting Twitter Trends collection...")
        results = []

        headers = self.get_rapidapi_headers(RAPIDAPI_HOSTS["twitter_trends"])
        base_url = "https://twitter-trends-api.p.rapidapi.com"

        # Get available places
        try:
            url = f"{base_url}/places"
            data = await self.fetch_json(url, headers=headers)
            results.append({
                "source": "twitter_trends",
                "type": "places",
                "data": data
            })
            self.logger.info(f"  Twitter places: OK")
        except Exception as e:
            self.logger.error(f"  Twitter places failed: {e}")

        # Get trends for key locations (WOEIDs)
        woeids = {
            "1": "worldwide",
            "23424977": "usa",
            "23424975": "gb",
            "23424829": "germany",
            "23424819": "france"
        }

        for woeid, location in woeids.items():
            try:
                url = f"{base_url}/"
                params = {"woeid": woeid}
                data = await self.fetch_json(url, headers=headers, params=params)
                results.append({
                    "source": "twitter_trends",
                    "type": "trends",
                    "location": location,
                    "woeid": woeid,
                    "data": data
                })
                self.logger.info(f"  Twitter trends {location}: OK")
                await asyncio.sleep(0.5)
            except Exception as e:
                self.logger.error(f"  Twitter trends {location} failed: {e}")

        self.logger.info(f"Twitter Trends complete: {len(results)} datasets")
        return results


# ============================================================================
# SPORTS HIGHLIGHTS COLLECTOR
# ============================================================================

class SportsHighlightsCollector(BaseCollector):
    """Collect sports highlights to correlate with TV viewing"""

    async def collect(self) -> List[Dict]:
        """Collect sports highlights data"""
        self.logger.info("Starting Sports Highlights collection...")
        results = []

        headers = self.get_rapidapi_headers(RAPIDAPI_HOSTS["sports_highlights"])
        base_url = "https://sport-highlights-api.p.rapidapi.com"

        # Get highlights for various sports
        sports = ["football", "basketball", "hockey", "baseball"]

        for sport in sports:
            try:
                url = f"{base_url}/highlights/{sport}"
                params = {"date": datetime.now().strftime("%Y-%m-%d")}
                data = await self.fetch_json(url, headers=headers, params=params)
                results.append({
                    "source": "sports_highlights",
                    "type": f"{sport}_highlights",
                    "data": data
                })
                self.logger.info(f"  Sports {sport}: OK")
                await asyncio.sleep(0.3)
            except Exception as e:
                self.logger.error(f"  Sports {sport} failed: {e}")

        self.logger.info(f"Sports Highlights complete: {len(results)} datasets")
        return results


# ============================================================================
# REUTERS NEWS COLLECTOR
# ============================================================================

class ReutersCollector(BaseCollector):
    """Collect business/financial news from Reuters"""

    async def collect(self) -> List[Dict]:
        """Collect Reuters news data"""
        self.logger.info("Starting Reuters collection...")
        results = []

        headers = self.get_rapidapi_headers(RAPIDAPI_HOSTS["reuters"])
        base_url = "https://reuters-business-and-financial-news.p.rapidapi.com"

        # Category IDs for entertainment/media
        categories = {
            "240": "technology",
            "241": "media",
            "242": "business"
        }

        today = datetime.now().strftime("%Y-%m-%d")

        for cat_id, cat_name in categories.items():
            try:
                url = f"{base_url}/category-id/{cat_id}/article-date/{today}/0/20"
                data = await self.fetch_json(url, headers=headers)
                results.append({
                    "source": "reuters",
                    "type": f"news_{cat_name}",
                    "category_id": cat_id,
                    "data": data
                })
                self.logger.info(f"  Reuters {cat_name}: OK")
                await asyncio.sleep(0.3)
            except Exception as e:
                self.logger.error(f"  Reuters {cat_name} failed: {e}")

        self.logger.info(f"Reuters complete: {len(results)} datasets")
        return results


# ============================================================================
# MAIN COLLECTOR ORCHESTRATOR
# ============================================================================

class ScaredChildGatherer:
    """Main orchestrator for all data collection"""

    def __init__(self, config: APIConfig = None, scheduled: bool = False):
        self.config = config or APIConfig()
        self.scheduled = scheduled
        self.logger = setup_logging(self.config)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.date_str = datetime.now().strftime("%Y-%m-%d")
        self.all_results: Dict[str, List[Dict]] = {}

    def log_header(self):
        """Print collection header"""
        self.logger.info("=" * 70)
        self.logger.info("SCARED CHILD GATHERER - Nightly Data Collection")
        self.logger.info("=" * 70)
        self.logger.info(f"Collection Date: {self.date_str}")
        self.logger.info(f"Timestamp: {self.timestamp}")
        self.logger.info(f"Mode: {'Scheduled' if self.scheduled else 'Manual'}")
        self.logger.info("=" * 70)

    async def collect_all_async(self):
        """Run all async collectors"""
        async with aiohttp.ClientSession() as session:
            # TMDB
            tmdb = TMDBCollector(self.config, self.logger)
            tmdb.session = session
            self.all_results["tmdb"] = await tmdb.collect()

            # Streaming Availability
            streaming = StreamingAvailabilityCollector(self.config, self.logger)
            streaming.session = session
            self.all_results["streaming_availability"] = await streaming.collect()

            # IMDB
            imdb = IMDBCollector(self.config, self.logger)
            imdb.session = session
            self.all_results["imdb"] = await imdb.collect()

            # Rotten Tomatoes
            rt = RottenTomatoesCollector(self.config, self.logger)
            rt.session = session
            self.all_results["rotten_tomatoes"] = await rt.collect()

            # HBO Max
            hbo = HBOMaxCollector(self.config, self.logger)
            hbo.session = session
            self.all_results["hbo_max"] = await hbo.collect()

            # Disney+
            disney = DisneyPlusCollector(self.config, self.logger)
            disney.session = session
            self.all_results["disney_plus"] = await disney.collect()

            # Seeking Alpha
            seeking = SeekingAlphaCollector(self.config, self.logger)
            seeking.session = session
            self.all_results["seeking_alpha"] = await seeking.collect()

            # Twitter Trends
            twitter = TwitterTrendsCollector(self.config, self.logger)
            twitter.session = session
            self.all_results["twitter_trends"] = await twitter.collect()

            # Sports Highlights
            sports = SportsHighlightsCollector(self.config, self.logger)
            sports.session = session
            self.all_results["sports_highlights"] = await sports.collect()

            # Reuters
            reuters = ReutersCollector(self.config, self.logger)
            reuters.session = session
            self.all_results["reuters"] = await reuters.collect()

    def collect_sync(self):
        """Run synchronous collectors"""
        # FlixPatrol (requires sync Basic Auth)
        flixpatrol = FlixPatrolCollector(self.config, self.logger)
        self.all_results["flixpatrol"] = flixpatrol.collect_sync()

    def save_individual_json(self):
        """Save each source to individual JSON files"""
        for source, data in self.all_results.items():
            if data:
                filename = f"{source}_{self.timestamp}.json"
                filepath = Path(self.config.OUTPUT_DIR) / filename
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, default=str)
                self.logger.info(f"Saved: {filename}")

    def save_combined_json(self):
        """Save all data to combined fresh_data JSON"""
        combined = {
            "collection_date": datetime.now().isoformat(),
            "date_range": {
                "start": (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
                "end": datetime.now().strftime("%Y-%m-%d")
            },
            "total_sources": len(self.all_results),
            "results": []
        }

        for source, data_list in self.all_results.items():
            for item in data_list:
                item["_source_category"] = source
                combined["results"].append(item)

        filename = f"fresh_data_{self.timestamp}.json"
        filepath = Path(self.config.OUTPUT_DIR) / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(combined, f, indent=2, default=str)
        self.logger.info(f"Saved combined: {filename}")

        return combined

    def create_parquet(self, combined_data: Dict):
        """Create consolidated parquet file"""
        records = []

        for result in combined_data.get("results", []):
            source = result.get("source", result.get("_source_category", "unknown"))
            data_type = result.get("type", result.get("endpoint", "unknown"))
            data = result.get("data", {})

            # Extract items if they exist
            items = []
            if isinstance(data, dict):
                items = data.get("results", data.get("data", []))
            elif isinstance(data, list):
                items = data

            if isinstance(items, list):
                for item in items[:100]:  # Limit per source
                    if isinstance(item, dict):
                        record = {
                            "collection_date": combined_data["collection_date"],
                            "source": source,
                            "type": data_type,
                            "id": item.get("id", item.get("imdb_id", item.get("tmdbId", ""))),
                            "title": item.get("title", item.get("name", "")),
                            "year": item.get("release_date", item.get("premiere", ""))[:4] if item.get("release_date") or item.get("premiere") else "",
                            "popularity": item.get("popularity", item.get("vote_count", 0)),
                            "rating": item.get("vote_average", item.get("imdb_score", 0)),
                            "raw_data": json.dumps(item, default=str)[:2000]  # Truncate
                        }
                        records.append(record)

        if records:
            df = pd.DataFrame(records)

            # Save parquet
            parquet_file = Path(self.config.OUTPUT_DIR) / f"cranberry_fresh_{self.timestamp}.parquet"
            df.to_parquet(parquet_file, index=False)
            self.logger.info(f"Saved parquet: {parquet_file.name} ({len(df)} records)")

            # Save CSV summary
            csv_file = Path(self.config.OUTPUT_DIR) / f"fresh_data_summary_{self.timestamp}.csv"
            summary = df.drop(columns=['raw_data']).copy()
            summary.to_csv(csv_file, index=False)
            self.logger.info(f"Saved CSV summary: {csv_file.name}")

            # Also save full CSV
            csv_full = Path(self.config.OUTPUT_DIR) / f"cranberry_fresh_{self.timestamp}.csv"
            df.to_csv(csv_full, index=False)

            return df
        return None

    def create_enhanced_collection(self, combined_data: Dict):
        """Create enhanced collection JSON with metadata"""
        enhanced = {
            "collection_metadata": {
                "gatherer_version": "1.0.0",
                "collection_timestamp": datetime.now().isoformat(),
                "scheduled_run": self.scheduled,
                "source_count": len(self.all_results),
                "total_records": sum(len(v) for v in self.all_results.values())
            },
            "api_status": {},
            "data": combined_data
        }

        # Track API success status
        for source, data in self.all_results.items():
            enhanced["api_status"][source] = {
                "success": len(data) > 0,
                "record_count": len(data)
            }

        filename = f"enhanced_collection_{self.timestamp}.json"
        filepath = Path(self.config.OUTPUT_DIR) / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(enhanced, f, indent=2, default=str)
        self.logger.info(f"Saved enhanced: {filename}")

    def update_xml_config(self):
        """Update/create ViewerDBX_DailyCollection.xml"""
        xml_path = Path(self.config.OUTPUT_DIR) / "ViewerDBX_DailyCollection.xml"

        root = ET.Element("ViewerDBX_DailyCollection")
        root.set("version", "1.0")
        root.set("last_run", datetime.now().isoformat())

        # Configuration
        config_elem = ET.SubElement(root, "Configuration")
        ET.SubElement(config_elem, "OutputDirectory").text = self.config.OUTPUT_DIR
        ET.SubElement(config_elem, "ScheduledTime").text = "02:00"
        ET.SubElement(config_elem, "RetentionDays").text = "30"

        # APIs
        apis_elem = ET.SubElement(root, "APIs")
        api_list = [
            ("TMDB", "api.themoviedb.org", "Active"),
            ("FlixPatrol", "api.flixpatrol.com", "Active"),
            ("StreamingAvailability", "streaming-availability.p.rapidapi.com", "Active"),
            ("IMDB", "imdb-api17.p.rapidapi.com", "Active"),
            ("RottenTomatoes", "rottentomato.p.rapidapi.com", "Active"),
            ("HBOMax", "hbo-max-movies-and-tv-shows-api-by-apirobots.p.rapidapi.com", "Active"),
            ("DisneyPlus", "disney-plus-top-movies-and-tv-shows-api-by-apirobots.p.rapidapi.com", "Active"),
            ("SeekingAlpha", "seeking-alpha.p.rapidapi.com", "Active"),
            ("TwitterTrends", "twitter-trends-api.p.rapidapi.com", "Active"),
            ("SportsHighlights", "sport-highlights-api.p.rapidapi.com", "Active"),
            ("Reuters", "reuters-business-and-financial-news.p.rapidapi.com", "Active"),
        ]

        for name, host, status in api_list:
            api = ET.SubElement(apis_elem, "API")
            api.set("name", name)
            ET.SubElement(api, "Host").text = host
            ET.SubElement(api, "Status").text = status

        # Last Collection Status
        status_elem = ET.SubElement(root, "LastCollection")
        ET.SubElement(status_elem, "Timestamp").text = self.timestamp
        ET.SubElement(status_elem, "TotalSources").text = str(len(self.all_results))
        ET.SubElement(status_elem, "TotalRecords").text = str(sum(len(v) for v in self.all_results.values()))

        # Countries
        countries_elem = ET.SubElement(root, "TargetCountries")
        for country in TARGET_COUNTRIES:
            ET.SubElement(countries_elem, "Country").text = country.upper()

        # Write XML
        tree = ET.ElementTree(root)
        ET.indent(tree, space="  ")
        tree.write(xml_path, encoding="utf-8", xml_declaration=True)
        self.logger.info(f"Updated XML config: ViewerDBX_DailyCollection.xml")

    async def run(self):
        """Main execution method"""
        start_time = time.time()
        self.log_header()

        try:
            # Run async collectors
            self.logger.info("\n--- Starting Async Collections ---")
            await self.collect_all_async()

            # Run sync collectors
            self.logger.info("\n--- Starting Sync Collections ---")
            self.collect_sync()

            # Save outputs
            self.logger.info("\n--- Saving Output Files ---")
            self.save_individual_json()
            combined = self.save_combined_json()
            self.create_parquet(combined)
            self.create_enhanced_collection(combined)
            self.update_xml_config()

            # Summary
            elapsed = time.time() - start_time
            self.logger.info("\n" + "=" * 70)
            self.logger.info("COLLECTION COMPLETE")
            self.logger.info("=" * 70)
            self.logger.info(f"Total Sources: {len(self.all_results)}")
            self.logger.info(f"Total Datasets: {sum(len(v) for v in self.all_results.values())}")
            self.logger.info(f"Elapsed Time: {elapsed:.1f} seconds")
            self.logger.info("=" * 70)

        except Exception as e:
            self.logger.error(f"Collection failed: {e}")
            raise


# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Scared Child Gatherer - Data Collection System")
    parser.add_argument("--scheduled", action="store_true", help="Running as scheduled task")
    parser.add_argument("--test", action="store_true", help="Run in test mode (limited APIs)")
    args = parser.parse_args()

    gatherer = ScaredChildGatherer(scheduled=args.scheduled)
    asyncio.run(gatherer.run())


if __name__ == "__main__":
    main()
