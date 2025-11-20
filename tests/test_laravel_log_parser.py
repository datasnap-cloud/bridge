import unittest
from sync.extractor import _extract_laravel_log_records


SAMPLE = (
    "[2025-11-19 23:25:20] local.INFO: Executando SETs de sessão ClickHouse  \n"
    "[2025-11-19 23:25:20] local.INFO: Executando query ClickHouse {\"query\":\"SELECT amount\"} \n"
    "[2025-11-19 23:25:21] production.ERROR: Falha ao executar  \n"
)


class TestLaravelLogParser(unittest.TestCase):
    def test_parse_records_from_text_chunk(self):
        mapping = {"source": {"type": "laravel_log", "path": "", "max_memory_mb": 50}}
        # monkeypatch: feed text via temporary file
        import tempfile, os
        with tempfile.NamedTemporaryFile(delete=False, mode="wb") as tf:
            tf.write(SAMPLE.encode("utf-8"))
            temp_path = tf.name
        try:
            mapping["source"]["path"] = temp_path
            records = _extract_laravel_log_records(mapping)
            assert records is not None
            assert len(records) >= 2
            assert records[0]["environment"] == "local"
            assert records[0]["type"] == "INFO"
            assert "Executando SETs" in records[0]["message"]
            assert records[1]["type"] in ("INFO", "ERROR")
        finally:
            os.unlink(temp_path)

