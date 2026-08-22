"""Idempotent, streaming compaction for completed Parquet partitions."""

from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path

from atomic_io import atomic_write_json, fsync_directory, read_json
from parquet_store import parquet_row_count


def _align_table(table, schema):
    import pyarrow as pa

    arrays = []
    for field in schema:
        if field.name in table.column_names:
            column = table[field.name]
            if column.type != field.type:
                column = column.cast(field.type)
        else:
            column = pa.nulls(len(table), type=field.type)
        arrays.append(column)
    return pa.Table.from_arrays(arrays, schema=schema)


def compact_partition(data_dir: Path, feed_name: str, date_str: str, *, min_files: int = 12) -> Path | None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    directory = data_dir / "parquet" / feed_name / f"date={date_str}"
    if not directory.exists():
        return None
    # A collector may still be completing an old failed spool commit. Waiting
    # avoids compacting away the deterministic output needed for its recovery.
    spool_partition = data_dir / "spool" / feed_name / f"date={date_str}"
    if spool_partition.exists() and any(spool_partition.iterdir()):
        return None
    marker = directory / ".compaction-transaction.json"
    transaction = read_json(marker, {})
    if transaction:
        sources = [directory / name for name in transaction["sources"]]
        output = directory / transaction["output"]
        expected_rows = int(transaction["expected_rows"])
        if output.exists() and parquet_row_count(output) == expected_rows:
            for source in sources:
                if source != output:
                    try:
                        source.unlink()
                    except FileNotFoundError:
                        pass
            marker.unlink()
            fsync_directory(directory)
            return output
        if any(not source.exists() for source in sources):
            raise RuntimeError("incomplete compaction transaction has missing sources and no valid output")
    else:
        sources = sorted(directory.glob("*.parquet"))
        if len(sources) < min_files:
            return None
        expected_rows = sum(parquet_row_count(path) for path in sources)
        identity = "\n".join(f"{path.name}:{path.stat().st_size}" for path in sources).encode("utf-8")
        digest = hashlib.sha256(identity).hexdigest()[:20]
        output = directory / f"part-compacted-{digest}.parquet"
        transaction = {
            "version": 1,
            "sources": [path.name for path in sources],
            "output": output.name,
            "expected_rows": expected_rows,
        }
        atomic_write_json(marker, transaction)

    schemas = [pq.ParquetFile(path).schema_arrow.remove_metadata() for path in sources]
    schema = pa.unify_schemas(schemas)
    temporary = directory / f".{output.name}.tmp-{uuid.uuid4().hex}"
    writer = None
    try:
        writer = pq.ParquetWriter(temporary, schema, compression="zstd", write_statistics=True)
        for source in sources:
            parquet = pq.ParquetFile(source)
            for batch in parquet.iter_batches(batch_size=65_536):
                writer.write_table(_align_table(pa.Table.from_batches([batch]), schema))
        writer.close()
        writer = None
        with open(temporary, "rb") as handle:
            os.fsync(handle.fileno())
        if parquet_row_count(temporary) != expected_rows:
            raise IOError("compacted row count does not match source row count")
        os.replace(temporary, output)
        fsync_directory(directory)
        if parquet_row_count(output) != expected_rows:
            raise IOError("atomic compaction output failed validation")
        for source in sources:
            if source != output:
                source.unlink()
        marker.unlink()
        fsync_directory(directory)
        return output
    finally:
        if writer is not None:
            writer.close()
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
