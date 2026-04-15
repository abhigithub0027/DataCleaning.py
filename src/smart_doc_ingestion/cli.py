from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .pipeline import DocumentIngestionConfig, DocumentIngestionPipeline


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest .txt/.pdf files from a folder and chunk them semantically."
    )
    parser.add_argument("--input-dir", required=True, type=Path, help="Folder containing .txt/.pdf files")
    parser.add_argument("--output-file", required=True, type=Path, help="Destination JSON or CSV file")
    parser.add_argument(
        "--output-format",
        default="json",
        choices=("json", "csv"),
        help="Output format for generated chunks",
    )
    parser.add_argument(
        "--max-chunk-chars",
        type=int,
        default=4500,
        help="Approximate max chunk size in characters (~800-1000 tokens)",
    )
    parser.add_argument(
        "--min-chunk-chars",
        type=int,
        default=1200,
        help="Soft lower bound before a chunk is flushed",
    )
    parser.add_argument(
        "--overlap-chars",
        type=int,
        default=400,
        help="Approximate trailing overlap retained for the next chunk",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Console logging level",
    )
    return parser


def configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def main() -> list[dict]:
    parser = build_arg_parser()
    args = parser.parse_args()
    configure_logging(args.log_level)

    config = DocumentIngestionConfig(
        input_dir=args.input_dir,
        output_file=args.output_file,
        output_format=args.output_format,
        max_chunk_chars=args.max_chunk_chars,
        min_chunk_chars=args.min_chunk_chars,
        overlap_chars=args.overlap_chars,
    )
    return DocumentIngestionPipeline(config).run()


if __name__ == "__main__":
    main()
