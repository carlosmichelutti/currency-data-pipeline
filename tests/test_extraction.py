import json
import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from requests import Response, Session

from scripts.extract import (
    FrankfurterAPIClient,
    FrankfurterAPIError,
    persist_json,
    run_extract,
)


def make_response(payload: object) -> Mock:
    """Create a successful mocked requests response.

    Args:
        payload (object): Python value returned when the response JSON is decoded.

    Returns:
        unittest.mock.Mock: Response mock configured with the supplied payload.
    """

    response = Mock(spec=Response)
    response.status_code = 200
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


class FrankfurterAPIClientTests(unittest.TestCase):
    def test_get_rates_builds_query_and_returns_records(self) -> None:
        expected = [
            {"date": "2026-09-28", "base": "BRL", "quote": "USD", "rate": 0.19}
        ]
        session = Mock(spec=Session)
        session.get.return_value = make_response(expected)

        result = FrankfurterAPIClient(timeout=10, session=session).get_rates(
            base="brl",
            quotes=["usd", "eur"],
            start_date=date(2026, 9, 1),
            end_date="2026-09-28",
        )

        self.assertEqual(result, expected)
        session.get.assert_called_once_with(
            url="https://api.frankfurter.dev/v2/rates",
            params={
                "base": "BRL",
                "quotes": "USD,EUR",
                "from": "2026-09-01",
                "to": "2026-09-28",
            },
            headers={
                "Accept": "application/json",
                "User-Agent": "currency-data-pipeline/0.1",
            },
            timeout=10,
        )

    def test_get_currencies_returns_catalogue(self) -> None:
        expected = [
            {
                "iso_code": "BRL",
                "iso_numeric": "986",
                "name": "Brazilian Real",
                "symbol": "R$",
                "start_date": "1994-07-01",
                "end_date": "2026-09-28",
            }
        ]
        session = Mock(spec=Session)
        session.get.return_value = make_response(expected)

        result = FrankfurterAPIClient(session=session).get_currencies(
            include_legacy=True,
            providers="ecb",
        )

        self.assertEqual(result, expected)
        session.get.assert_called_once_with(
            url="https://api.frankfurter.dev/v2/currencies",
            params={"scope": "all", "providers": "ECB"},
            headers={
                "Accept": "application/json",
                "User-Agent": "currency-data-pipeline/0.1",
            },
            timeout=30.0,
        )

    def test_rejects_single_date_combined_with_range(self) -> None:
        with self.assertRaisesRegex(ValueError, "date cannot be combined"):
            FrankfurterAPIClient().get_rates(
                date="2026-09-28",
                start_date="2026-09-01",
            )

    def test_rejects_inverted_range(self) -> None:
        with self.assertRaisesRegex(ValueError, "start_date cannot be after"):
            FrankfurterAPIClient().get_rates(
                start_date="2026-09-28",
                end_date="2026-09-01",
            )

    def test_rejects_unexpected_rates_payload(self) -> None:
        session = Mock(spec=Session)
        session.get.return_value = make_response({"rates": []})

        with self.assertRaisesRegex(RuntimeError, "unexpected rates payload"):
            FrankfurterAPIClient(session=session).get_rates()

    def test_logs_http_request_failure(self) -> None:
        response = Response()
        response.status_code = 422
        response.reason = "Unprocessable Entity"
        response.url = "https://api.frankfurter.dev/v2/rates"
        response._content = b'{"message": "invalid currency: ABC"}'
        session = Mock(spec=Session)
        session.get.return_value = response

        with (
            self.assertLogs("scripts.extract", level="ERROR") as logs,
            self.assertRaisesRegex(FrankfurterAPIError, "HTTP 422"),
        ):
            FrankfurterAPIClient(session=session).get_rates(base="ABC")

        self.assertIn("invalid currency: ABC", " ".join(logs.output))

    def test_rejects_incomplete_currency_record(self) -> None:
        session = Mock(spec=Session)
        session.get.return_value = make_response([{"iso_code": "BRL"}])

        with self.assertRaisesRegex(RuntimeError, "incomplete currency record"):
            FrankfurterAPIClient(session=session).get_currencies()

    def test_persist_json_writes_utf8_payload(self) -> None:
        payload = [{"iso_code": "BRL", "name": "Real brasileiro", "symbol": "R$"}]

        with TemporaryDirectory() as directory:
            destination = Path(directory) / "nested" / "currencies.json"
            result = persist_json(payload, destination)

            self.assertEqual(Path(result), destination.resolve())
            self.assertEqual(
                json.loads(destination.read_text(encoding="utf-8")),
                payload,
            )

    @patch("scripts.extract.extract_rates")
    @patch("scripts.extract.extract_currencies")
    def test_run_extract_persists_both_endpoints(
        self,
        mock_extract_currencies,
        mock_extract_rates,
    ) -> None:
        currencies = [{"iso_code": "BRL", "name": "Brazilian Real"}]
        rates = [
            {"date": "2026-09-28", "base": "BRL", "quote": "USD", "rate": 0.19}
        ]
        mock_extract_currencies.return_value = currencies
        mock_extract_rates.return_value = rates

        with (
            TemporaryDirectory() as directory,
            patch("scripts.extract.RAW_DIR", Path(directory)),
        ):
            result = run_extract(batch_id="20260928190000", base="BRL")

            currencies_path = Path(result["currencies_path"])
            rates_path = Path(result["rates_path"])
            self.assertEqual(result["batch_id"], "20260928190000")
            self.assertEqual(
                json.loads(currencies_path.read_text(encoding="utf-8")),
                currencies,
            )
            self.assertEqual(
                json.loads(rates_path.read_text(encoding="utf-8")),
                rates,
            )
            self.assertIn("batch_id=20260928190000", currencies_path.as_posix())
            self.assertIn("batch_id=20260928190000", rates_path.as_posix())

if __name__ == "__main__":
    unittest.main()
