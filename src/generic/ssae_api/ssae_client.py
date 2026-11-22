import base64
import time
from typing import Any, Dict, List, Optional, Tuple, Iterable

import requests

class SsaeApiError(Exception):
    """Generic SSAE error (HTTP or application-level)."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class SsaeApiClient:
    """
    SSAE MAS client (SetupTables + CDP) aligned with the provided documentation.

    - Handles authentication via /connect/token (client_credentials).
    - Provides low-level GET calls with basic retry logic.
    - Exposes one method per CDP endpoint.
    - Exposes a generic method for setup tables (no need to change the class
      when a new setup table is added).
    - Does NOT manage persistence of offsets, dates, or cursors.
    """

    def __init__(
        self,
        base_url: str,
        access_key: str,
        auth_key: str,
        timeout: int = 30,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        logger: Any = None,
        debug: bool = False,
    ):
        """
        :param base_url: e.g. "https://api.example.com"
        :param access_key: accessKey provided by SSAE admin.
        :param auth_key: authKey provided by SSAE admin.
        :param debug: If True, print debug steps with timestamps.
        """
        self.base_url = base_url.rstrip("/")
        self.access_key = access_key
        self.auth_key = auth_key
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.logger = logger
        self.debug = debug

        self._access_token: Optional[str] = None
        self._token_expires_at: Optional[float] = None

        self._session = requests.Session()

    # -------------------------------------------------------------------------
    # Logging
    # -------------------------------------------------------------------------

    def _log(self, level: str, message: str) -> None:
        if self.debug:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [{level.upper()}] {message}")
        if not self.logger:
            return
        fn = getattr(self.logger, level, None)
        if callable(fn):
            fn(message)

    # -------------------------------------------------------------------------
    # Authentication
    # -------------------------------------------------------------------------

    def _get_token(self) -> None:
        """
        Retrieve a new access token via /connect/token (client_credentials).
        """
        self._log("debug", "Starting token retrieval")
        url = f"{self.base_url}/connect/token"

        basic = base64.b64encode(
            f"{self.access_key}:{self.auth_key}".encode("utf-8")
        ).decode("ascii")

        headers = {
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {"grant_type": "client_credentials"}

        self._log("info", "SSAE: requesting new access token")

        start_time = time.time()
        resp = self._session.post(
            url,
            headers=headers,
            data=data,
            timeout=self.timeout,
        )
        elapsed = time.time() - start_time
        self._log("debug", f"Token request completed in {elapsed:.2f}s")

        if resp.status_code != 200:
            self._log("error", f"Failed to obtain token (status={resp.status_code})")
            raise SsaeApiError(
                f"Failed to obtain token (status={resp.status_code})",
                status_code=resp.status_code,
                response=self._safe_json(resp),
            )

        body = self._safe_json(resp) or {}
        result = body.get("result") or {}
        token = result.get("access_token")
        expires_in = result.get("expires_in", 3600)

        if not token:
            self._log("error", "Token missing in /connect/token response")
            raise SsaeApiError(
                "Token missing in /connect/token response",
                response=body,
            )

        self._access_token = token
        # Subtract 60 seconds as a safety margin
        self._token_expires_at = time.time() + expires_in - 60
        self._log("debug", f"Token set, expires in {expires_in}s")

    def _ensure_token(self) -> None:
        """
        Ensure there is a valid (non-expired) access token.
        """
        if self._access_token is None or self._token_expires_at is None:
            self._log("debug", "No access token, fetching new one")
            self._get_token()
            return
        if time.time() >= self._token_expires_at:
            self._log("info", "SSAE: access token expired, refreshing")
            self._get_token()
        else:
            self._log("debug", "Access token is still valid")

    # -------------------------------------------------------------------------
    # Low-level HTTP
    # -------------------------------------------------------------------------

    @staticmethod
    def _safe_json(resp: requests.Response) -> Optional[Dict[str, Any]]:
        try:
            return resp.json()
        except Exception:
            return None

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Generic HTTP request wrapper.

        :param path: relative path, e.g. "/api/mas/cdp/EU/sites"
        :return: full SSAE JSON wrapper (including result, page, cursor, etc.).
        """
        self._log("debug", f"Preparing request: {method} {path} params={params}")
        self._ensure_token()

        url = f"{self.base_url}{path}"
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/json",
        }

        attempt = 0
        while True:
            attempt += 1
            try:
                self._log("debug", f"HTTP {method} {url} (attempt {attempt})")
                start_time = time.time()
                resp = self._session.request(
                    method=method.upper(),
                    url=url,
                    headers=headers,
                    params=params,
                    timeout=self.timeout,
                )
                elapsed = time.time() - start_time
                self._log("debug", f"HTTP {method} {url} completed in {elapsed:.2f}s (status={resp.status_code})")
            except requests.RequestException as exc:
                if attempt > self.max_retries:
                    self._log("error", f"Network error: {exc}")
                    raise SsaeApiError(f"Network error: {exc}") from exc
                sleep = self.backoff_factor ** (attempt - 1)
                self._log(
                    "warning",
                    f"Network error attempt {attempt}, retrying in {sleep}s: {exc}",
                )
                time.sleep(sleep)
                continue

            # HTTP success
            if 200 <= resp.status_code < 300:
                data = self._safe_json(resp) or {}                
                self._log("debug", f"Request successful: {method} {url}")
                return data

            # 401 -> refresh token and retry
            if resp.status_code == 401 and attempt <= self.max_retries:
                self._log("warning", "401 received, refreshing token and retrying")
                self._get_token()
                headers["Authorization"] = f"Bearer {self._access_token}"
                continue

            # Transient errors
            if resp.status_code in (429, 500, 502, 503, 504) and attempt <= self.max_retries:
                sleep = self.backoff_factor ** (attempt - 1)
                self._log(
                    "warning",
                    f"Transient error (status={resp.status_code}) attempt "
                    f"{attempt}, retrying in {sleep}s",
                )
                time.sleep(sleep)
                continue

            data = self._safe_json(resp)
            self._log("error", f"HTTP error from SSAE (status={resp.status_code})")
            raise SsaeApiError(
                f"HTTP error from SSAE (status={resp.status_code} , text ={resp.text})",
                status_code=resp.status_code,
                response=data,
            )

    # -------------------------------------------------------------------------
    # Pagination helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _extract_result(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract the 'result' list from the SSAE wrapper.
        """
        return data.get("result") or []

    # -------------------------------------------------------------------------
    # SETUP TABLES (generic)
    # -------------------------------------------------------------------------

    def get_setup_table(
        self,
        country: str,
        table_name: str,
    ) -> List[Dict[str, Any]]:
        """
        GET /api/mas/setup/{country}/{tableName}

        This is generic: you can call any setup table by name.
        Example:
            get_setup_table("EU", "agencies")
            get_setup_table("EU", "siteTypes")
        """
        self._log("debug", f"Getting setup table: {country}/{table_name}")
        path = f"/api/mas/setup/{country}/{table_name}"
        data = self._request("GET", path)
        return self._extract_result(data)

    # -------------------------------------------------------------------------
    # CDP Endpoints: Generic low-level helpers
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # Generic method to endpoints based on page number, page size
    def _get_page_based(
        self,
        *,
        country: str,
        endpoint: str,
        page_number: int,
        page_size: int,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        path = f"/api/mas/cdp/{country}/{endpoint}"
        params: Dict[str, Any] = {
            "pageNumber": page_number,
            "pageSize": page_size,
        }
        if from_date:
            params["fromDate"] = from_date
        if to_date:
            params["toDate"] = to_date

        self._log("debug", f"GET {path} params={params}")
        return self._request("GET", path, params=params)
    
    # -------------------------------------------------------------------------
    # _iterate method based on page_number/page_size
    def _iterate_pages(
        self,
        fetch_page_func,
        *args,
        **kwargs,
    ) -> Iterable[Dict[str, Any]]:
        """
        Iterate over a page-based endpoint (pageNumber / pageCount).

        fetch_page_func must accept page_number and return the SSAE wrapper.
        """
        page_number = 1
        while True:
            self._log("debug", f"Fetching page {page_number}")
            data = fetch_page_func(page_number=page_number, *args, **kwargs)
            result = self._extract_result(data)
            if not result:
                self._log("debug", f"No more results at page {page_number}")
                break
            for row in result:
                yield row

            page = data.get("page", page_number)
            page_count = data.get("pageCount", page_number)
            if page >= page_count:
                self._log("debug", f"Reached last page {page}")
                break
            page_number += 1


    # -------------------------------------------------------------------------
    # Generic method to endpoints based on fetch, offset
    def _get_offset_based(
        self,
        *,
        country: str,
        endpoint: str,
        from_date: str,
        to_date: str,
        fetch: int,
        offset: int,
    ) -> Dict[str, Any]:
        path = f"/api/mas/cdp/{country}/{endpoint}"
        params = {
            "fetch": fetch,
            "offset": offset,
            "fromDate": from_date,
            "toDate": to_date,
        }

        self._log("debug", f"GET {path} params={params}")
        return self._request("GET", path, params=params)
       
    # -------------------------------------------------------------------------
    # _iterate method based on fetch, offset
    def _iterate_offset(
        self,
        fetch_page_func,
        *args,
        **kwargs,
    ) -> Iterable[Dict[str, Any]]:
        """
        Iterate over an offset-based endpoint (fetch / offset / maxRows).

        According to the docs:
        - offset starts at 0
        - offset is incremented by fetch
        - stop when result is empty OR offset > maxRows
        """
        offset = 0
        while True:
            self._log("debug", f"Fetching offset {offset}")
            data = fetch_page_func(offset=offset, *args, **kwargs)
            result = self._extract_result(data)
            if not result:
                self._log("debug", f"No more results at offset {offset}")
                break

            for row in result:
                yield row

            max_rows = data.get("maxRows")
            fetch = kwargs.get("fetch")
            if fetch is None:
                # Fallback: use the size of the current result
                fetch = len(result)

            offset += fetch
            if max_rows is not None and offset >= max_rows:
                self._log("debug", f"Reached maxRows {max_rows} at offset {offset}")
                break



    # ---------------------------------------------------------------------
    # CDP: SITES
    # ---------------------------------------------------------------------

    def get_sites_page(
        self,
        country: str,
        page_number: int = 1,
        page_size: int = 10000,
        from_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self._get_page_based(
            country=country,
            endpoint="sites",
            page_number=page_number,
            page_size=page_size,
            from_date=from_date,
        )

    def get_sites(
        self,
        country: str,
        page_size: int = 10000,
        from_date: Optional[str] = None,  
    ) -> List[Dict[str, Any]]:
        return list(
            self._iterate_pages(
                self.get_sites_page,
                country=country,
                page_size=page_size,
                from_date=from_date,
            )
        )

    def get_uniquesite(
        self,
        country: str,
        site_no: int,
    ) -> Optional[Dict[str, Any]]:
        path = f"/api/mas/cdp/{country}/sites/{site_no}"
        self._log("debug", f"GET {path}")
        data = self._request("GET", path)
        result = self._extract_result(data)
        return result if result else None

    def get_uniquesite_systems(
        self,
        country: str,
        site_no: int,
    ) -> List[Dict[str, Any]]:
        path = f"/api/mas/cdp/{country}/sites/{site_no}/systems"
        self._log("debug", f"GET {path}")
        data = self._request("GET", path)
        return self._extract_result(data)

    def get_site_changes_page(
        self,
        country: str,
        from_date: str,
        to_date: str,
        fetch: int = 10000,
        offset: int = 0,
    ) -> Dict[str, Any]:
        return self._get_offset_based(
            country=country,
            endpoint="sites/changes",
            from_date=from_date,
            to_date=to_date,
            fetch=fetch,
            offset=offset,
        )

    
    def get_site_changes(
        self,
        country: str,
        from_date: str,
        to_date: str,
        fetch: int = 10000,
    ) -> List[Dict[str, Any]]:
        return list(
            self._iterate_offset(
                self.get_site_changes_page,
                country=country,
                from_date=from_date,
                to_date=to_date,
                fetch=fetch,
            )
        )


    def get_site_deletes_page(
        self,
        country: str,
        from_date: str,
        to_date: str,
        fetch: int = 10000,
        offset: int = 0,
    ) -> Dict[str, Any]:
        return self._get_offset_based(
            country=country,
            endpoint="sites/deletes",
            from_date=from_date,
            to_date=to_date,
            fetch=fetch,
            offset=offset,
        )

    def get_site_deletes(
        self,
        country: str,
        from_date: str,
        to_date: str,
        fetch: int = 10000,
    ) -> List[Dict[str, Any]]:
        return list(
            self._iterate_offset(
                self.get_site_deletes_page,
                country=country,
                from_date=from_date,
                to_date=to_date,
                fetch=fetch,
            )
        )

    # ---------------------------------------------------------------------
    # CDP: SYSTEMS
    # ---------------------------------------------------------------------

    def get_systems_page(
        self,
        country: str,
        page_number: int = 1,
        page_size: int = 10000,
        from_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self._get_page_based(
            country=country,
            endpoint="systems",
            page_number=page_number,
            page_size=page_size,
            from_date=from_date,
        )

    def get_systems(
        self,
        country: str,
        page_size: int = 10000,
        from_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return list(
            self._iterate_pages(
                self.get_systems_page,
                country=country,
                page_size=page_size,
                from_date=from_date,
            )
        )

    def get_system_changes_page(
        self,
        country: str,
        from_date: str,
        to_date: str,
        fetch: int = 10000,
        offset: int = 0,
    ) -> Dict[str, Any]:
        return self._get_offset_based(
            country=country,
            endpoint="systems/changes",
            from_date=from_date,
            to_date=to_date,
            fetch=fetch,
            offset=offset,
        )

    def get_system_changes(
        self,
        country: str,
        from_date: str,
        to_date: str,
        fetch: int = 10000,
    ) -> List[Dict[str, Any]]:
        return list(
            self._iterate_offset(
                self.get_system_changes_page,
                country=country,
                from_date=from_date,
                to_date=to_date,
                fetch=fetch,
            )
        )

    def get_system_deletes_page(
        self,
        country: str,
        from_date: str,
        to_date: str,
        fetch: int = 10000,
        offset: int = 0,
    ) -> Dict[str, Any]:
        return self._get_offset_based(
            country=country,
            endpoint="systems/deletes",
            from_date=from_date,
            to_date=to_date,
            fetch=fetch,
            offset=offset,
        )

    def get_system_deletes(
        self,
        country: str,
        from_date: str,
        to_date: str,
        fetch: int = 10000,
    ) -> List[Dict[str, Any]]:
        return list(
            self._iterate_offset(
                self.get_system_deletes_page,
                country=country,
                from_date=from_date,
                to_date=to_date,
                fetch=fetch,
            )
        )

    # ---------------------------------------------------------------------
    # CDP: ALARM INCIDENTS
    # ---------------------------------------------------------------------

    def get_alarm_incidents_page(
        self,
        country: str,
        from_date: str,
        to_date: str,
        page_number: int = 1,
        page_size: int = 10000,
    ) -> Dict[str, Any]:
        return self._get_page_based(
            country=country,
            endpoint="alarmIncidents",
            page_number=page_number,
            page_size=page_size,
            from_date=from_date,
            to_date=to_date,
        )

    def get_alarm_incidents(
    self,
    country: str,
    from_date: str,
    to_date: str,
    page_size: int = 10000,
    ) -> List[Dict[str, Any]]:
        return list(
            self._iterate_pages(
                self.get_alarm_incidents_page,
                country=country,
                from_date=from_date,
                to_date=to_date,
                page_size=page_size,
            )
        )

    def get_alarm_incident_changes_page(
        self,
        country: str,
        from_date: str,
        to_date: str,
        fetch: int = 10000,
        offset: int = 0,
    ) -> Dict[str, Any]:
        return self._get_offset_based(
            country=country,
            endpoint="alarmIncidents/changes",
            from_date=from_date,
            to_date=to_date,
            fetch=fetch,
            offset=offset,
        )

    def get_alarm_incident_changes(
        self,
        country: str,
        from_date: str,
        to_date: str,
        fetch: int = 10000,
    ) -> List[Dict[str, Any]]:
        return list(
            self._iterate_offset(
                self.get_alarm_incident_changes_page,
                country=country,
                from_date=from_date,
                to_date=to_date,
                fetch=fetch,
            )
        )

    # ---------------------------------------------------------------------
    # CDP: ALARM INCIDENTS CALL
    # ---------------------------------------------------------------------

    def get_alarm_incidentcalls_page(
        self,
        country: str,
        from_date: str,
        to_date: str,
        page_number: int = 1,
        page_size: int = 10000,
    ) -> Dict[str, Any]:
        return self._get_page_based(
            country=country,
            endpoint="alarmIncidentCalls",
            page_number=page_number,
            page_size=page_size,
            from_date=from_date,
            to_date=to_date,
        )

    def get_alarm_incidentcalls(
        self,
        country: str,
        from_date: str,
        to_date: str,
        page_size: int = 10000,
    ) -> List[Dict[str, Any]]:
        return list(
            self._iterate_pages(
                self.get_alarm_incidentcalls_page,
                country=country,
                from_date=from_date,
                to_date=to_date,
                page_size=page_size,
            )
        )


    def get_alarm_incidentcalls_changes_page(
        self,
        country: str,
        from_date: str,
        to_date: str,
        fetch: int = 10000,
        offset: int = 0,
    ) -> Dict[str, Any]:
        return self._get_offset_based(
            country=country,
            endpoint="alarmIncidentcalls/changes",
            from_date=from_date,
            to_date=to_date,
            fetch=fetch,
            offset=offset,
        )

    def get_alarm_incidentcalls_changes(
    self,
    country: str,
    from_date: str,
    to_date: str,
    fetch: int = 10000,
    ) -> List[Dict[str, Any]]:
        return list(
            self._iterate_offset(
                self.get_alarm_incidentcalls_changes_page,
                country=country,
                from_date=from_date,
                to_date=to_date,
                fetch=fetch,
            )
        )


    # ---------------------------------------------------------------------
    # CDP: EVENT HISTORIES (cursor-based)
    # ---------------------------------------------------------------------

    def get_event_histories_page(
        self,
        country: str,
        from_date: str,
        to_date: str,
        page_number: int = 1,
        page_size: int = 10000,
    ) -> Dict[str, Any]:
        return self._get_page_based(
            country=country,
            endpoint="eventHistories",
            page_number=page_number,
            page_size=page_size,
            from_date=from_date,
            to_date=to_date,
        )

    def get_event_histories(
        self,
        country: str,
        from_date: str,
        to_date: str,
        page_size: int = 10000,
    ) -> List[Dict[str, Any]]:
        return list(
            self._iterate_pages(
                self.get_event_histories_page,
                country=country,
                from_date=from_date,
                to_date=to_date,
                page_size=page_size,
            )
        )

    def get_event_history_cursor(
        self,
        country: str,
        to_date: Optional[str] = None,
    ) -> str:
        path = f"/api/mas/cdp/{country}/eventHistories/changes/cursor"
        params = {"toDate": to_date} if to_date else {}
        data = self._request("GET", path, params=params)

        if "cursor" in data:
            return data["cursor"]

        cursor = (data.get("result") or {}).get("cursor")
        if not cursor:
            raise SsaeApiError("Cursor missing", response=data)
        return cursor

    def get_event_history_changes(
        self,
        country: str,
        cursor: str,
    ) -> Tuple[List[Dict[str, Any]], str]:
        path = f"/api/mas/cdp/{country}/eventHistories/changes/{cursor}"
        data = self._request("GET", path)
        return self._extract_result(data), data.get("cursor", cursor)

    # ---------------------------------------------------------------------
    # CDP: RESTRICTIONS
    # ---------------------------------------------------------------------

    def get_restriction_inclusions_page(
        self,
        country: str,
        page_number: int = 1,
        page_size: int = 10000,
        from_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self._get_page_based(
            country=country,
            endpoint="restrictions/inclusions",
            page_number=page_number,
            page_size=page_size,
            from_date=from_date,
        )

    def get_restriction_inclusions(
        self,
        country: str,
        page_size: int = 10000,
        from_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return list(
            self._iterate_pages(
                self.get_restriction_inclusions_page,
                country=country,
                page_size=page_size,
                from_date=from_date,
            )
        )

    def get_restriction_exclusions_page(
        self,
        country: str,
        page_number: int = 1,
        page_size: int = 10000,
        from_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self._get_page_based(
            country=country,
            endpoint="restrictions/exclusions",
            page_number=page_number,
            page_size=page_size,
            from_date=from_date,
        )

    def get_restriction_exclusions(
        self,
        country: str,
        page_size: int = 10000,
        from_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return list(
            self._iterate_pages(
                self.get_restriction_exclusions_page,
                country=country,
                page_size=page_size,
                from_date=from_date,
            )
        )

    def get_restriction_changes_page(
        self,
        country: str,
        from_date: str,
        to_date: str,
        fetch: int = 10000,
        offset: int = 0,
    ) -> Dict[str, Any]:
        return self._get_offset_based(
            country=country,
            endpoint="restrictions/changes",
            from_date=from_date,
            to_date=to_date,
            fetch=fetch,
            offset=offset,
        )

    def get_restriction_changes(
        self,
        country: str,
        from_date: str,
        to_date: str,
        fetch: int = 10000,
    ) -> List[Dict[str, Any]]:
        return list(
            self._iterate_offset(
                self.get_restriction_changes_page,
                country=country,
                from_date=from_date,
                to_date=to_date,
                fetch=fetch,
            )
        )
    