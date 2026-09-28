"""Tests for snapshot ingestion and conservative change classification."""

import tempfile
import unittest
from datetime import date
from pathlib import Path
from typing import Iterable, Tuple

import duckdb

from src.pipeline import run_pipeline


Row = Tuple[object, ...]


def _write_parquet(path: Path, rows: Iterable[Row]) -> None:
    connection = duckdb.connect()
    try:
        connection.execute("""
        CREATE TABLE fixture (
            id_cliente VARCHAR,
            date VARCHAR,
            product VARCHAR,
            type VARCHAR,
            fund VARCHAR,
            amount DOUBLE,
            description VARCHAR,
            commercial_name VARCHAR
        )
        """)
        connection.executemany(
            "INSERT INTO fixture VALUES (?, ?, ?, ?, ?, ?, ?, ?)", list(rows)
        )
        target = str(path).replace("'", "''")
        connection.execute("COPY fixture TO '{}' (FORMAT PARQUET)".format(target))
    finally:
        connection.close()


def _row(
    client: str,
    date: str,
    product: str,
    movement_type: str,
    fund: str,
    amount: float,
    description: str,
    commercial_name: str = "Broker",
) -> Row:
    return (
        client,
        date,
        product,
        movement_type,
        fund,
        amount,
        description,
        commercial_name,
    )


class PipelineTests(unittest.TestCase):
    def test_classifies_snapshots_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "raw"
            output = root / "output"
            source.mkdir()
            _write_parquet(
                source / "movimientos_dia_T.parquet",
                [
                    _row("C1", "2024-09-30", "P1", "IN", "F1", 10.0, "same"),
                    _row("C2", "01/10/2024", "P2", "IN", "F2", 20.0, "before"),
                    _row("C3", "2024-10-02", "P3", "OUT", "F3", -5.0, "removed"),
                ],
            )
            _write_parquet(
                source / "movimientos_dia_T1.parquet",
                [
                    _row("C1", "30/09/2024", "P1", "IN", "F1", 10.0, "same"),
                    _row("C2", "2024-10-01", "P2", "OUT", "F2", 25.0, "after"),
                    _row("C4", "2024-10-04", "P4", "IN", "F4", 12.0, "new"),
                ],
            )

            run_pipeline(source, output)
            connection = duckdb.connect(str(output / "tyba.duckdb"), read_only=True)
            try:
                events = dict(
                    connection.execute(
                        "SELECT event_type, count(*) FROM change_events GROUP BY event_type"
                    ).fetchall()
                )
                self.assertEqual(
                    events,
                    {"UNCHANGED": 1, "CORRECTED": 1, "NEW": 1, "REMOVED": 1},
                )
                current = connection.execute(
                    "SELECT count(*), min(movement_date), max(movement_date) FROM current_movements"
                ).fetchone()
                self.assertEqual(current, (3, date(2024, 9, 30), date(2024, 10, 4)))
            finally:
                connection.close()

            first_report = (output / "insights.json").read_text(encoding="utf-8")
            run_pipeline(source, output)
            second_report = (output / "insights.json").read_text(encoding="utf-8")
            self.assertEqual(first_report, second_report)
            connection = duckdb.connect(str(output / "tyba.duckdb"), read_only=True)
            try:
                self.assertEqual(
                    connection.execute("SELECT count(*) FROM snapshots").fetchone()[0], 2
                )
                self.assertEqual(
                    connection.execute("SELECT count(*) FROM change_events").fetchone()[0], 4
                )
            finally:
                connection.close()

    def test_ambiguous_composite_key_is_not_guessed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "raw"
            output = root / "output"
            source.mkdir()
            shared = {"client": "C1", "date": "2024-09-30", "product": "P1", "fund": "F1"}
            _write_parquet(
                source / "cut_T.parquet",
                [
                    _row(shared["client"], shared["date"], shared["product"], "IN", shared["fund"], 1.0, "one"),
                    _row(shared["client"], shared["date"], shared["product"], "OUT", shared["fund"], 2.0, "two"),
                ],
            )
            _write_parquet(
                source / "cut_T1.parquet",
                [_row(shared["client"], shared["date"], shared["product"], "IN", shared["fund"], 3.0, "changed")],
            )

            run_pipeline(source, output)
            connection = duckdb.connect(str(output / "tyba.duckdb"), read_only=True)
            try:
                result = connection.execute(
                    "SELECT event_type, old_count, new_count FROM change_events"
                ).fetchall()
                self.assertEqual(result, [("AMBIGUOUS", 2, 1)])
            finally:
                connection.close()

    def test_rejects_schema_that_does_not_match_provided_data(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "raw"
            output = root / "output"
            source.mkdir()
            connection = duckdb.connect()
            try:
                connection.execute("CREATE TABLE wrong_schema (id VARCHAR)")
                target = str(source / "wrong.parquet").replace("'", "''")
                connection.execute(
                    "COPY wrong_schema TO '{}' (FORMAT PARQUET)".format(target)
                )
            finally:
                connection.close()

            with self.assertRaisesRegex(ValueError, "Unsupported Parquet schema"):
                run_pipeline(source, output)


if __name__ == "__main__":
    unittest.main()