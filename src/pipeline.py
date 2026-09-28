"""Load Parquet movement snapshots into DuckDB and report their evolution."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

import duckdb


LOGGER = logging.getLogger("tyba_pipeline")
REQUIRED_COLUMNS = (
    "id_cliente",
    "date",
    "product",
    "type",
    "fund",
    "amount",
    "description",
    "commercial_name",
)

_CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_key VARCHAR PRIMARY KEY,
    source_name VARCHAR NOT NULL,
    content_sha256 VARCHAR NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_row_count BIGINT NOT NULL DEFAULT 0,
    snapshot_order INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS snapshot_rows (
    snapshot_key VARCHAR NOT NULL,
    movement_key VARCHAR NOT NULL,
    record_hash VARCHAR NOT NULL,
    id_cliente VARCHAR,
    source_date VARCHAR,
    movement_date DATE,
    product VARCHAR,
    type VARCHAR,
    fund VARCHAR,
    amount DOUBLE,
    description VARCHAR,
    commercial_name VARCHAR
);

CREATE TABLE IF NOT EXISTS change_events (
    from_snapshot_key VARCHAR NOT NULL,
    to_snapshot_key VARCHAR NOT NULL,
    movement_key VARCHAR NOT NULL,
    event_type VARCHAR NOT NULL,
    old_count BIGINT NOT NULL,
    new_count BIGINT NOT NULL,
    old_record_hash VARCHAR,
    new_record_hash VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""

_DATE_EXPRESSION = """coalesce(
    try_strptime(trim(date), '%Y-%m-%d')::DATE,
    try_strptime(trim(date), '%d/%m/%Y')::DATE
)"""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _snapshot_key(source_name: str, content_sha256: str) -> str:
    identity = "\0".join((source_name, content_sha256)).encode("utf-8")
    return hashlib.sha256(identity).hexdigest()


def _natural_sort_key(value: str) -> Tuple[Any, ...]:
    stem = Path(value).stem
    suffix = Path(value).suffix.lower()
    parts = tuple(
        (1, int(part)) if part.isdigit() else (0, part.lower())
        for part in re.split(r"(\d+)", stem)
    )
    return parts + ((0, suffix),)


def _validate_schema(connection: duckdb.DuckDBPyConnection, path: Path) -> None:
    columns = [
        row[0]
        for row in connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
        ).fetchall()
    ]
    missing = sorted(set(REQUIRED_COLUMNS) - set(columns))
    unexpected = sorted(set(columns) - set(REQUIRED_COLUMNS))
    if missing or unexpected:
        details = []
        if missing:
            details.append("missing required columns: " + ", ".join(missing))
        if unexpected:
            details.append("unexpected columns: " + ", ".join(unexpected))
        raise ValueError(
            "Unsupported Parquet schema in " + path.name + " (" + "; ".join(details) + ")"
        )


def _insert_snapshot(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
    content_sha256: str,
    snapshot_key: str,
) -> int:
    _validate_schema(connection, path)
    connection.execute(
        "INSERT INTO snapshots (snapshot_key, source_name, content_sha256) VALUES (?, ?, ?)",
        [snapshot_key, path.name, content_sha256],
    )

    insert_sql = f"""
    INSERT INTO snapshot_rows (
        snapshot_key, movement_key, record_hash, id_cliente, source_date,
        movement_date, product, type, fund, amount, description, commercial_name
    )
    WITH typed AS (
        SELECT
            id_cliente,
            date AS source_date,
            {_DATE_EXPRESSION} AS movement_date,
            product,
            type,
            fund,
            amount,
            description,
            commercial_name
        FROM read_parquet(?)
    ), hashed AS (
        SELECT
            *,
            sha256(to_json(struct_pack(
                id_cliente := id_cliente,
                movement_date := coalesce(CAST(movement_date AS VARCHAR), trim(source_date)),
                product := product,
                fund := fund,
                commercial_name := commercial_name
            ))) AS movement_key,
            sha256(to_json(struct_pack(
                id_cliente := id_cliente,
                movement_date := coalesce(CAST(movement_date AS VARCHAR), trim(source_date)),
                product := product,
                type := type,
                fund := fund,
                amount := amount,
                description := description,
                commercial_name := commercial_name
            ))) AS record_hash
        FROM typed
    )
    SELECT ?, movement_key, record_hash, id_cliente, source_date, movement_date,
           product, type, fund, amount, description, commercial_name
    FROM hashed
    """
    connection.execute(insert_sql, [str(path), snapshot_key])
    row_count = connection.execute(
        "SELECT count(*) FROM snapshot_rows WHERE snapshot_key = ?", [snapshot_key]
    ).fetchone()[0]
    connection.execute(
        "UPDATE snapshots SET source_row_count = ? WHERE snapshot_key = ?",
        [row_count, snapshot_key],
    )
    return int(row_count)


def _resequence_snapshots(connection: duckdb.DuckDBPyConnection) -> List[str]:
    snapshots = connection.execute(
        "SELECT snapshot_key, source_name, loaded_at FROM snapshots"
    ).fetchall()
    snapshots.sort(key=lambda row: (_natural_sort_key(row[1]), row[2], row[0]))
    for order, (snapshot_key, _source_name, _loaded_at) in enumerate(snapshots):
        connection.execute(
            "UPDATE snapshots SET snapshot_order = ? WHERE snapshot_key = ?",
            [order, snapshot_key],
        )
    return [row[0] for row in snapshots]


def _rebuild_change_events(
    connection: duckdb.DuckDBPyConnection, snapshot_keys: Sequence[str]
) -> None:
    connection.execute("DELETE FROM change_events")
    insert_sql = """
    INSERT INTO change_events (
        from_snapshot_key, to_snapshot_key, movement_key, event_type,
        old_count, new_count, old_record_hash, new_record_hash
    )
    SELECT
        ?,
        ?,
        coalesce(old.movement_key, new.movement_key),
        CASE
            WHEN coalesce(old.record_count, 0) > 1 OR coalesce(new.record_count, 0) > 1
                THEN 'AMBIGUOUS'
            WHEN old.record_count IS NULL THEN 'NEW'
            WHEN new.record_count IS NULL THEN 'REMOVED'
            WHEN old.record_hash = new.record_hash THEN 'UNCHANGED'
            ELSE 'CORRECTED'
        END,
        coalesce(old.record_count, 0),
        coalesce(new.record_count, 0),
        CASE WHEN old.record_count = 1 THEN old.record_hash ELSE NULL END,
        CASE WHEN new.record_count = 1 THEN new.record_hash ELSE NULL END
    FROM (
        SELECT movement_key, count(*) AS record_count, min(record_hash) AS record_hash
        FROM snapshot_rows
        WHERE snapshot_key = ?
        GROUP BY movement_key
    ) AS old
    FULL OUTER JOIN (
        SELECT movement_key, count(*) AS record_count, min(record_hash) AS record_hash
        FROM snapshot_rows
        WHERE snapshot_key = ?
        GROUP BY movement_key
    ) AS new USING (movement_key)
    """
    for prior_key, current_key in zip(snapshot_keys, snapshot_keys[1:]):
        connection.execute(
            insert_sql, [prior_key, current_key, prior_key, current_key]
        )


def _create_current_view(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("""
    CREATE OR REPLACE VIEW current_movements AS
    SELECT rows.*, snapshots.source_name AS snapshot_name
    FROM snapshot_rows AS rows
    JOIN snapshots USING (snapshot_key)
    WHERE snapshots.snapshot_order = (
        SELECT max(snapshot_order) FROM snapshots
    )
    """)


def _load_new_snapshots(
    connection: duckdb.DuckDBPyConnection, input_dir: Path
) -> List[str]:
    paths = sorted(input_dir.glob("*.parquet"), key=lambda path: _natural_sort_key(path.name))
    if not paths:
        raise FileNotFoundError("No .parquet snapshots found in " + str(input_dir))

    connection.execute("BEGIN TRANSACTION")
    try:
        for path in paths:
            content_sha256 = _sha256_file(path)
            exists = connection.execute(
                "SELECT 1 FROM snapshots WHERE source_name = ? AND content_sha256 = ?",
                [path.name, content_sha256],
            ).fetchone()
            if exists:
                LOGGER.info("Skipping already loaded snapshot %s", path.name)
                continue
            snapshot_key = _snapshot_key(path.name, content_sha256)
            row_count = _insert_snapshot(
                connection, path, content_sha256, snapshot_key
            )
            LOGGER.info("Loaded %s (%s rows)", path.name, row_count)

        snapshot_keys = _resequence_snapshots(connection)
        _rebuild_change_events(connection, snapshot_keys)
        _create_current_view(connection)
        connection.execute("COMMIT")
        return snapshot_keys
    except Exception:
        connection.execute("ROLLBACK")
        raise


def _type_totals(connection: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    rows = connection.execute("""
    SELECT
        CASE
            WHEN upper(trim(type)) IN ('IN', 'ENTRADA') THEN 'IN'
            WHEN upper(trim(type)) IN ('OUT', 'SALIDA') THEN 'OUT'
            ELSE coalesce('UNMAPPED: ' || upper(trim(type)), 'UNMAPPED: NULL')
        END AS normalized_type,
        count(*) AS row_count,
        sum(amount) AS amount_total,
        count(*) FILTER (WHERE amount IS NULL) AS null_amount_count
    FROM current_movements
    GROUP BY normalized_type
    ORDER BY normalized_type
    """).fetchall()
    return [
        {
            "type": row[0],
            "rows": int(row[1]),
            "amount_total": float(row[2]) if row[2] is not None else None,
            "null_amounts": int(row[3]),
        }
        for row in rows
    ]


def _build_insights(connection: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    latest = connection.execute("""
    SELECT source_name, source_row_count
    FROM snapshots
    ORDER BY snapshot_order DESC
    LIMIT 1
    """).fetchone()
    if latest is None:
        raise RuntimeError("No snapshots were loaded")

    quality = connection.execute("""
    SELECT
        count(*) AS rows,
        count(DISTINCT id_cliente) AS distinct_clients,
        sum(amount) AS amount_total,
        count(*) FILTER (WHERE id_cliente IS NULL) AS null_id_cliente,
        count(*) FILTER (WHERE source_date IS NULL OR length(trim(source_date)) = 0) AS null_date,
        count(*) FILTER (WHERE product IS NULL) AS null_product,
        count(*) FILTER (WHERE type IS NULL) AS null_type,
        count(*) FILTER (WHERE fund IS NULL) AS null_fund,
        count(*) FILTER (WHERE amount IS NULL) AS null_amount,
        count(*) FILTER (WHERE description IS NULL) AS null_description,
        count(*) FILTER (WHERE commercial_name IS NULL) AS null_commercial_name,
        count(*) FILTER (WHERE movement_date IS NULL) AS unparsed_date,
        count(*) FILTER (WHERE regexp_full_match(trim(source_date), '[0-9]{4}-[0-9]{2}-[0-9]{2}')) AS iso_date_strings,
        count(*) FILTER (WHERE regexp_full_match(trim(source_date), '[0-9]{2}/[0-9]{2}/[0-9]{4}')) AS day_first_date_strings,
        count(*) FILTER (WHERE amount < 0) AS negative_amounts,
        count(*) FILTER (WHERE amount = 0) AS zero_amounts
    FROM current_movements
    """).fetchone()
    fields = (
        "rows",
        "distinct_clients",
        "amount_total",
        "null_id_cliente",
        "null_date",
        "null_product",
        "null_type",
        "null_fund",
        "null_amount",
        "null_description",
        "null_commercial_name",
        "unparsed_date",
        "iso_date_strings",
        "day_first_date_strings",
        "negative_amounts",
        "zero_amounts",
    )
    current_metrics: Dict[str, Any] = {}
    for name, value in zip(fields, quality):
        if name == "amount_total":
            current_metrics[name] = float(value) if value is not None else None
        else:
            current_metrics[name] = int(value)
    current_metrics["source_name"] = latest[0]

    change_rows = connection.execute("""
    SELECT old.source_name, new.source_name, events.event_type, count(*)
    FROM change_events AS events
    JOIN snapshots AS old ON old.snapshot_key = events.from_snapshot_key
    JOIN snapshots AS new ON new.snapshot_key = events.to_snapshot_key
    GROUP BY old.source_name, new.source_name, old.snapshot_order,
             new.snapshot_order, events.event_type
    ORDER BY old.snapshot_order, new.snapshot_order, events.event_type
    """).fetchall()
    changes_by_pair: Dict[Tuple[str, str], Dict[str, int]] = {}
    for source_from, source_to, event_type, count in change_rows:
        changes_by_pair.setdefault((source_from, source_to), {})[event_type] = int(count)
    changes = [
        {
            "from_snapshot": source_from,
            "to_snapshot": source_to,
            "events": counts,
        }
        for (source_from, source_to), counts in changes_by_pair.items()
    ]

    return {
        "current_snapshot": current_metrics,
        "movement_type_totals": _type_totals(connection),
        "snapshot_changes": changes,
        "identity_note": (
            "Los archivos proporcionados contienen id_cliente, no el identificador de transacción "
            "descrito en el PDF. Por ello, movement_key es una clave compuesta candidata basada "
            "en cliente, fecha normalizada, producto, fondo y nombre comercial. Las claves "
            "duplicadas se clasifican como AMBIGUOUS en vez de adivinar una correspondencia. "
            "Sin un identificador de transacción estable no se pueden emparejar de forma fiable "
            "los cambios en los campos que forman la clave."
        ),
    }


def _write_insights(output_dir: Path, insights: Dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "insights.json").write_text(
        json.dumps(insights, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    current = insights["current_snapshot"]
    lines = [
        "# Insights del pipeline",
        "",
        "## Corte más reciente",
        "",
        "- Archivo: `{}`".format(current["source_name"]),
        "- Filas: {:,}".format(current["rows"]),
        "- Identificadores de cliente distintos: {:,}".format(current["distinct_clients"]),
        "- Suma de montos no nulos: {}".format(
            "no disponible" if current["amount_total"] is None else "{:,.2f}".format(current["amount_total"])
        ),
        "- Montos negativos: {:,}; montos en cero: {:,}".format(
            current["negative_amounts"], current["zero_amounts"]
        ),
        "",
        "## Suma de montos por tipo de movimiento normalizado",
        "",
        "Los montos se suman tal como vienen en los archivos; los valores nulos se excluyen de la suma.",
        "",
        "| Tipo | Filas | Monto total | Montos nulos |",
        "| --- | ---: | ---: | ---: |",
    ]
    for item in insights["movement_type_totals"]:
        amount = "—" if item["amount_total"] is None else "{:,.2f}".format(item["amount_total"])
        lines.append(
            "| {} | {:,} | {} | {:,} |".format(
                item["type"], item["rows"], amount, item["null_amounts"]
            )
        )

    quality_labels = {
        "null_id_cliente": "id_cliente",
        "null_date": "date",
        "null_product": "product",
        "null_type": "type",
        "null_fund": "fund",
        "null_amount": "amount",
        "null_description": "description",
        "null_commercial_name": "commercial_name",
    }
    lines.extend(
        [
            "",
            "## Calidad de los datos",
            "",
            "| Validación | Filas |",
            "| --- | ---: |",
        ]
    )
    for key, label in quality_labels.items():
        lines.append("| `{}` nulo | {:,} |".format(label, current[key]))
    lines.extend(
        [
            "| Fechas no interpretables | {:,} |".format(current["unparsed_date"]),
            "| Fechas con formato `YYYY-MM-DD` | {:,} |".format(current["iso_date_strings"]),
            "| Fechas con formato `DD/MM/YYYY` | {:,} |".format(current["day_first_date_strings"]),
            "",
            "Los dos formatos de fecha observados se convierten a una fecha normalizada. Se conservan los montos nulos y con signo; no se corrigen silenciosamente.",
            "",
            "## Cambios entre cortes",
            "",
            "`CORRECTED` indica que la clave candidata coincide de forma única, pero cambió otro campo. Las claves `AMBIGUOUS` no se emparejan automáticamente.",
            "",
        ]
    )
    if insights["snapshot_changes"]:
        lines.extend(
            [
                "| Desde | Hasta | Evento | Claves candidatas |",
                "| --- | --- | --- | ---: |",
            ]
        )
        event_labels = {
            "UNCHANGED": "Sin cambios (UNCHANGED)",
            "CORRECTED": "Corregido (CORRECTED)",
            "NEW": "Nuevo (NEW)",
            "REMOVED": "Eliminado (REMOVED)",
            "AMBIGUOUS": "Ambiguo (AMBIGUOUS)",
        }
        for comparison in insights["snapshot_changes"]:
            for event, count in sorted(comparison["events"].items()):
                lines.append(
                    "| {} | {} | {} | {:,} |".format(
                        comparison["from_snapshot"],
                        comparison["to_snapshot"],
                        event_labels.get(event, event),
                        count,
                    )
                )
    else:
        lines.append("No hay un corte anterior disponible para comparar.")
    lines.extend(["", "## Limitación de identidad", "", insights["identity_note"], ""])
    (output_dir / "insights.md").write_text("\n".join(lines), encoding="utf-8")


def run_pipeline(input_dir: Path, output_dir: Path) -> Dict[str, Any]:
    input_dir = input_dir.resolve()
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    database_path = output_dir / "tyba.duckdb"

    with duckdb.connect(str(database_path)) as connection:
        connection.execute(_CREATE_TABLES_SQL)
        snapshot_keys = _load_new_snapshots(connection, input_dir)
        if not snapshot_keys:
            raise RuntimeError("No snapshots are available after ingestion")
        insights = _build_insights(connection)

    _write_insights(output_dir, insights)
    LOGGER.info("Database and insights written under %s", output_dir)
    return insights


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/output"))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    run_pipeline(args.input_dir, args.output_dir)


if __name__ == "__main__":
    main()