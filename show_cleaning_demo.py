from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from smart_doc_ingestion.pipeline import DocumentIngestionConfig, DocumentIngestionPipeline


def main() -> None:
    sample_file = ROOT / "sample_input" / "dirty_sample.txt"
    raw_text = sample_file.read_text(encoding="utf-8", errors="ignore")

    config = DocumentIngestionConfig(
        input_dir=ROOT / "sample_input",
        output_file=ROOT / "output" / "cleaning_demo.json",
    )
    pipeline = DocumentIngestionPipeline(config)
    cleaned_text = pipeline._clean_text(raw_text)

    print("SOURCE FILE :", sample_file)
    print()
    print("UNCLEAN DATA:")
    print("-" * 60)
    print(raw_text)
    print()
    print("CLEAN DATA:")
    print("-" * 60)
    print(cleaned_text)


if __name__ == "__main__":
    main()
