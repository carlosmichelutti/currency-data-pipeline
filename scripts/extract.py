import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date as Date
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import requests

BASE_URL = 'https://api.frankfurter.dev/v2'
USER_AGENT = 'currency-data-pipeline/0.1'

TIMEZONE = timezone(timedelta(hours=-3), name='UTC-03:00')

LOGGER = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT_DIR / 'data' / 'raw'


class FrankfurterAPIError(RuntimeError):
    """
    Represent an error raised while communicating with the Frankfurter API.
    """


@dataclass(slots=True)
class FrankfurterAPIClient:
    """
    Provide access to the rates and currencies endpoints of Frankfurter API v2.

    Attributes:
        base_url (str): Base URL prepended to every API endpoint.
        timeout (float): Maximum number of seconds allowed for each request.
        session (requests.Session): Reusable HTTP session that sends requests.
    """
    base_url: str = BASE_URL
    timeout: float = 30.0
    session: requests.Session = field(default_factory=requests.Session, repr=False)

    def get_rates(
        self,
        base: str = 'EUR',
        quotes: str | Iterable[str] | None = None,
        date: str | Date | None = None,
        start_date: str | Date | None = None,
        end_date: str | Date | None = None,
        providers: str | Iterable[str] | None = None,
        group: str | None = None,
        expand_providers: bool = False
    ) -> list[dict[str, Any]]:
        """
        Fetch current or historical rates as one record per currency pair.

        Args:
            base (str): Base currency code used to calculate the rates.
            quotes (str | Iterable[str] | None): Quote currency codes to include.
                A comma-separated string and an iterable of strings are both accepted.
                When omitted, the API returns every available quote currency.
            date (str | Date | None): Exact rate date in YYYY-MM-DD format.
            start_date (str | Date | None): First date of a historical range.
            end_date (str | Date | None): Last date of a historical range.
            providers (str | Iterable[str] | None): Provider keys used to filter rates.
            group (str | None): Optional range grouping, either ``week`` or ``month``.
            expand_providers (bool): Whether each rate should include provider details.

        Returns:
            list[dict[str, Any]]: Rate records containing date, base, quote, and rate.

        Raises:
            ValueError: If date filters or query parameters are invalid.
            FrankfurterError: If the API request or response is invalid.
        """
        params = _build_rate_params(
            base=base,
            quotes=quotes,
            date=date,
            start_date=start_date,
            end_date=end_date,
            providers=providers,
            group=group,
            expand_providers=expand_providers
        )
        payload = self._get('rates', params)

        if not isinstance(payload, list) or not all(
            isinstance(record, dict) for record in payload
        ):
            LOGGER.error('Rates endpoint returned an unexpected payload')
            raise FrankfurterAPIError('Frankfurter returned an unexpected rates payload')

        required_fields = {'date', 'base', 'quote', 'rate'}
        if any(not required_fields.issubset(record) for record in payload):
            LOGGER.error('Rates endpoint returned an incomplete record')
            raise FrankfurterAPIError('Frankfurter returned an incomplete rate record')

        return payload

    def get_currencies(
        self,
        include_legacy: bool = False,
        providers: str | Iterable[str] | None = None
    ) -> list[dict[str, Any]]:
        """
        Fetch the currency catalogue and its publication coverage metadata.

        Args:
            include_legacy (bool): Whether inactive and historical currencies should
                be included in addition to active currencies.
            providers (str | Iterable[str] | None): Provider keys used to limit the
                currency catalogue. A comma-separated string or iterable is accepted.

        Returns:
            list[dict[str, Any]]: Currency records containing at least the ISO code
                and currency name, plus metadata supplied by the API.

        Raises:
            ValueError: If a provider value is empty.
            FrankfurterAPIError: If the currencies payload has an invalid structure.
        """
        params: dict[str, str] = {}
        if include_legacy:
            params['scope'] = 'all'

        normalized_providers = _join_codes(providers, 'providers')
        if normalized_providers:
            params['providers'] = normalized_providers

        payload = self._get('currencies', params)
        if not isinstance(payload, list) or not all(
            isinstance(record, dict) for record in payload
        ):
            LOGGER.error('Currencies endpoint returned an unexpected payload')
            raise FrankfurterAPIError(
                'Frankfurter returned an unexpected currencies payload'
            )

        required_fields = {'iso_code', 'name'}
        if any(not required_fields.issubset(record) for record in payload):
            LOGGER.error('Currencies endpoint returned an incomplete record')
            raise FrankfurterAPIError('Frankfurter returned an incomplete currency record')

        return payload

    def _get(self, path: str, params: dict[str, str]) -> Any:
        """
        Send a GET request and decode its JSON response.

        Args:
            path (str): API path relative to the configured base URL.
            params (dict[str, str]): Query-string parameters sent with the request.

        Returns:
            Any: JSON response decoded into Python objects.

        Raises:
            FrankfurterAPIError: If the connection, HTTP response, or JSON body is invalid.
        """
        url = f'{self.base_url.rstrip('/')}/{path.lstrip('/')}'
        LOGGER.info('Sending GET request: url=%s params=%s', url, params)
        try:
            response = self.session.get(
                url=url,
                params=params,
                headers={
                    'Accept': 'application/json',
                    'User-Agent': USER_AGENT
                },
                timeout=self.timeout
            )
            response.raise_for_status()
        except requests.HTTPError as error:
            error_response = error.response
            status_code = (
                error_response.status_code
                if error_response is not None
                else 'unknown'
            )
            message = _read_error_message(error_response)
            LOGGER.error(
                'GET request failed: url=%s status=%s message=%s',
                url,
                status_code,
                message
            )
            raise FrankfurterAPIError(
                f'Frankfurter request failed with HTTP {status_code}: {message}'
            )
        except requests.RequestException as error:
            LOGGER.error('GET request failed: url=%s error=%s', url, error)
            raise FrankfurterAPIError(f'Could not connect to Frankfurter: {error}')

        try:
            payload = response.json()
        except requests.JSONDecodeError:
            LOGGER.error(
                'GET request returned invalid JSON: url=%s status=%s',
                url,
                response.status_code
            )
            raise FrankfurterAPIError('Frankfurter returned invalid JSON')

        record_count = len(payload) if isinstance(payload, (dict, list)) else None
        LOGGER.info(
            'GET request succeeded: url=%s status=%s records=%s',
            url,
            response.status_code,
            record_count
        )
        return payload


def extract_rates(
    base: str = 'EUR',
    quotes: str | Iterable[str] | None = None,
    date: str | Date | None = None,
    start_date: str | Date | None = None,
    end_date: str | Date | None = None,
    providers: str | Iterable[str] | None = None,
    group: str | None = None,
    expand_providers: bool = False,
    timeout: float = 30.0
) -> list[dict[str, Any]]:
    """
    Extract the Frankfurter API rates.

    Args:
        base (str): Base currency code used to calculate the rates.
        quotes (str | Iterable[str] | None): Quote currency codes to include.
        date (str | Date | None): Exact rate date in YYYY-MM-DD format.
        start_date (str | Date | None): First date of a historical range.
        end_date (str | Date | None): Last date of a historical range.
        providers (str | Iterable[str] | None): Provider keys used to filter rates.
        group (str | None): Optional range grouping, either ``week`` or ``month``.
        expand_providers (bool): Whether each rate should include provider details.
        timeout (float): Maximum number of seconds allowed for the HTTP request.

    Returns:
        list[dict[str, Any]]: Current or historical exchange-rate records.
    """
    client = FrankfurterAPIClient(timeout=timeout)
    return client.get_rates(
        base=base,
        quotes=quotes,
        date=date,
        start_date=start_date,
        end_date=end_date,
        providers=providers,
        group=group,
        expand_providers=expand_providers
    )


def extract_currencies(
    include_legacy: bool = False,
    providers: str | Iterable[str] | None = None,
    timeout: float = 30.0
) -> list[dict[str, Any]]:
    """
    Extract the Frankfurter API currency catalogue.

    Args:
        include_legacy (bool): Whether historical currencies should be included.
        providers (str | Iterable[str] | None): Provider keys used to limit the
            currency catalogue.
        timeout (float): Maximum number of seconds allowed for the HTTP request.

    Returns:
        list[dict[str, Any]]: Currency records and their publication metadata.
    """
    client = FrankfurterAPIClient(timeout=timeout)
    return client.get_currencies(
        include_legacy=include_legacy,
        providers=providers
    )


def persist_json(data: Any, destination: str | Path) -> str:
    """
    Persist extracted data as a UTF-8 JSON file using an atomic replacement.

    Args:
        data (Any): JSON-serializable data returned by an extraction function.
        destination (str | Path): Local path where the JSON file should
            be persisted, including its file name.

    Returns:
        str: Absolute path of the persisted JSON file.
    """
    destination_path = Path(destination).resolve()
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = destination_path.with_name(
        f'.{destination_path.name}.{uuid4().hex}.tmp'
    )

    try:
        with temporary_path.open('w', encoding='utf-8', newline='\n') as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
            file.write('\n')
        temporary_path.replace(destination_path)
    except Exception:
        LOGGER.exception('Failed to persist JSON: path=%s', destination_path)
        temporary_path.unlink(missing_ok=True)
        raise

    LOGGER.info('JSON persisted successfully: path=%s', destination_path)
    return str(destination_path)


def run_extract(
    batch_id: str | None = None,
    base: str = 'BRL',
    quotes: str | Iterable[str] | None = None,
    include_legacy: bool = False,
    providers: str | Iterable[str] | None = None,
    timeout: float = 30.0
) -> dict[str, str]:
    """
    Extract currencies and rates and persist both raw JSON responses locally.

    Args:
        batch_id (str | None): Batch identifier formatted as YYYYMMDDHHMMSS.
            The current UTC-03:00 timestamp is used when omitted.
        base (str): Base currency code used to calculate the rates.
        quotes (str | Iterable[str] | None): Quote currency codes to include.
            When omitted, every available quote currency is extracted.
        include_legacy (bool): Whether historical currencies should be included.
        providers (str | Iterable[str] | None): Provider keys used to filter both
            endpoint requests.
        timeout (float): Maximum number of seconds allowed for each HTTP request.

    Returns:
        dict[str, str]: Batch identifier and absolute paths of the currencies and
            rates JSON files.
    """

    resolved_batch_id = batch_id or datetime.now(TIMEZONE).strftime(
        '%Y%m%d%H%M%S'
    )

    LOGGER.info(
        'Starting raw extraction: batch_id=%s output_root=%s',
        resolved_batch_id,
        RAW_DIR
    )

    currencies = extract_currencies(
        include_legacy=include_legacy,
        providers=providers,
        timeout=timeout
    )

    LOGGER.info(
        'Currencies extracted successfully: batch_id=%s records=%s',
        resolved_batch_id,
        len(currencies)
    )

    rates = extract_rates(
        base=base,
        quotes=quotes,
        providers=providers,
        timeout=timeout
    )

    LOGGER.info(
        'Rates extracted successfully: batch_id=%s records=%s base=%s',
        resolved_batch_id,
        len(rates),
        base.upper()
    )

    currencies_path = persist_json(
        currencies,
        RAW_DIR
        / 'currencies'
        / f'batch_id={resolved_batch_id}'
        / 'data.json'
    )

    rates_path = persist_json(
        rates,
        RAW_DIR
        / 'rates'
        / f'batch_id={resolved_batch_id}'
        / 'data.json'
    )

    LOGGER.info('Raw extraction completed successfully: batch_id=%s', resolved_batch_id)
    return {
        'batch_id': resolved_batch_id,
        'currencies_path': currencies_path,
        'rates_path': rates_path
    }


def _build_rate_params(
    base: str,
    quotes: str | Iterable[str] | None,
    date: str | Date | None,
    start_date: str | Date | None,
    end_date: str | Date | None,
    providers: str | Iterable[str] | None,
    group: str | None,
    expand_providers: bool
) -> dict[str, str]:
    """
    Validate rate filters and convert them into API query parameters.

    Args:
        base (str): Base currency code used to calculate the rates.
        quotes (str | Iterable[str] | None): Quote currency codes to include.
        date (str | Date | None): Exact rate date.
        start_date (str | Date | None): First date of a historical range.
        end_date (str | Date | None): Last date of a historical range.
        providers (str | Iterable[str] | None): Provider keys used to filter rates.
        group (str | None): Optional range grouping, either ``week`` or ``month``.
        expand_providers (bool): Whether provider details should be requested.

    Returns:
        dict[str, str]: Validated parameters using Frankfurter's query names.

    Raises:
        ValueError: If dates, currency codes, providers, or grouping are invalid.
    """
    if date is not None and (start_date is not None or end_date is not None):
        raise ValueError('date cannot be combined with start_date or end_date')

    normalized_start = _normalize_date(start_date, 'start_date')
    normalized_end = _normalize_date(end_date, 'end_date')
    if normalized_start and normalized_end and normalized_start > normalized_end:
        raise ValueError('start_date cannot be after end_date')

    normalized_group = group.lower() if group else None
    if normalized_group not in {None, 'week', 'month'}:
        raise ValueError('group must be either "week" or "month"')
    if normalized_group and not (normalized_start or normalized_end):
        raise ValueError('group can only be used with a date range')

    params = {'base': _normalize_code(base, 'base')}
    optional_params = {
        'quotes': _join_codes(quotes, 'quotes'),
        'date': _normalize_date(date, 'date'),
        'from': normalized_start,
        'to': normalized_end,
        'providers': _join_codes(providers, 'providers'),
        'group': normalized_group,
        'expand': 'providers' if expand_providers else None
    }
    params.update({key: value for key, value in optional_params.items() if value})
    return params


def _normalize_code(value: str, field_name: str) -> str:
    """
    Normalize and validate a currency or provider code.

    Args:
        value (str): Currency or provider code to normalize.
        field_name (str): Parameter name used in validation error messages.

    Returns:
        str: Trimmed, uppercase code.

    Raises:
        ValueError: If the normalized code is empty.
    """
    code = value.strip().upper()
    if not code:
        raise ValueError(f'{field_name} cannot be empty')
    return code


def _join_codes(
    values: str | Iterable[str] | None,
    field_name: str
) -> str | None:
    """
    Convert one or more codes into a normalized comma-separated value.

    Args:
        values (str | Iterable[str] | None): Codes supplied as a comma-separated
            string, an iterable of strings, or ``None``.
        field_name (str): Parameter name used in validation error messages.

    Returns:
        str | None: Uppercase comma-separated codes, or ``None`` when omitted.

    Raises:
        ValueError: If the collection or one of its codes is empty.
    """
    if values is None:
        return None

    raw_values = values.split(',') if isinstance(values, str) else values
    codes = [_normalize_code(value, field_name) for value in raw_values]
    if not codes:
        raise ValueError(f'{field_name} cannot be empty')
    return ','.join(codes)


def _normalize_date(value: str | Date | None, field_name: str) -> str | None:
    """
    Convert a supported date value into ISO YYYY-MM-DD format.

    Args:
        value (str | Date | None): Date value to normalize.
        field_name (str): Parameter name used in validation error messages.

    Returns:
        str | None: ISO-formatted date, or ``None`` when the value is omitted.

    Raises:
        ValueError: If the value is not a valid ISO date.
    """
    if value is None:
        return None
    if isinstance(value, Date):
        return value.isoformat()
    try:
        return Date.fromisoformat(value).isoformat()
    except (TypeError, ValueError):
        raise ValueError(f'{field_name} must use the YYYY-MM-DD format')


def _read_error_message(response: requests.Response | None) -> str:
    """
    Read the most useful error message from an HTTP response.

    Args:
        response (requests.Response | None): Failed response attached to an HTTP error.

    Returns:
        str: API-provided message when available, otherwise the HTTP reason.
    """
    if response is None:
        return 'unknown error'
    try:
        payload = response.json()
    except requests.JSONDecodeError:
        return response.reason or 'unknown error'
    if isinstance(payload, dict):
        return str(payload.get('message', response.reason or 'unknown error'))
    return response.reason or 'unknown error'


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s'
    )
    run_extract()
