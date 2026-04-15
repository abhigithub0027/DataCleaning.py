from pathlib import Path

from smart_doc_ingestion.pipeline import DocumentIngestionConfig, DocumentIngestionPipeline


def test_pipeline_generates_chunks(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_file = tmp_path / "output" / "chunks.json"
    input_dir.mkdir()
    (input_dir / "sample.txt").write_text(
        "First paragraph with useful context.\n\nSecond paragraph expands the topic.\n\nThird paragraph closes it.",
        encoding="utf-8",
    )

    config = DocumentIngestionConfig(
        input_dir=input_dir,
        output_file=output_file,
        max_chunk_chars=80,
        min_chunk_chars=30,
        overlap_chars=20,
    )

    chunks = DocumentIngestionPipeline(config).run()

    assert chunks
    assert output_file.exists()
    assert chunks[0]["file_name"] == "sample.txt"
    assert "chunk_text" in chunks[0]


def test_clean_text_removes_symbol_noise_but_keeps_common_punctuation(tmp_path: Path) -> None:
    config = DocumentIngestionConfig(
        input_dir=tmp_path,
        output_file=tmp_path / "output" / "chunks.json",
    )
    pipeline = DocumentIngestionPipeline(config)

    dirty_text = (
        "Budget   review@@@\n\n"
        "Policy notes%%% should stay readable!!!\n"
        "Percentages like 25% and R&D use & should remain."
    )

    cleaned = pipeline._clean_text(dirty_text)

    assert "@@@" not in cleaned
    assert "%%%" not in cleaned
    assert "Budget review" in cleaned
    assert "readable!!!" in cleaned
    assert "25%" in cleaned
    assert "R&D use & should remain." in cleaned
