from __future__ import annotations

import csv
import json
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

try:
    from pypdf import PdfReader
except ImportError:  # pragma: no cover - optional dependency
    PdfReader = None


LOGGER = logging.getLogger("smart_doc_ingestion")


@dataclass(slots=True)
class DocumentIngestionConfig:
    input_dir: Path
    output_file: Path
    output_format: str = "json"
    max_chunk_chars: int = 4500
    min_chunk_chars: int = 1200
    overlap_chars: int = 400
    min_expected_chunks: int = 1
    max_expected_chunks: int = 200


class DocumentIngestionPipeline:
    def __init__(self, config: DocumentIngestionConfig) -> None:
        self.config = config

    def run(self) -> list[dict]:
        start_time = time.perf_counter()
        files = self._discover_files()
        chunks: list[dict] = []
        processed_files = 0
        skipped_files = 0

        for file_path in files:
            try:
                raw_text = self._read_file(file_path)
            except Exception as exc:  # pragma: no cover - defensive logging
                skipped_files += 1
                LOGGER.warning("Skipping unreadable file %s: %s", file_path.name, exc)
                continue

            cleaned_text = self._clean_text(raw_text)
            if not cleaned_text.strip():
                skipped_files += 1
                LOGGER.warning("Skipping empty file %s", file_path.name)
                continue

            file_chunks = self._chunk_document(cleaned_text, file_path.name)
            processed_files += 1
            chunks.extend(file_chunks)

            if len(file_chunks) < self.config.min_expected_chunks:
                LOGGER.warning("File %s generated very few chunks: %s", file_path.name, len(file_chunks))
            if len(file_chunks) > self.config.max_expected_chunks:
                LOGGER.warning("File %s generated many chunks: %s", file_path.name, len(file_chunks))

        self._write_output(chunks)

        elapsed = time.perf_counter() - start_time
        LOGGER.info("Files discovered: %s", len(files))
        LOGGER.info("Files processed: %s", processed_files)
        LOGGER.info("Files skipped: %s", skipped_files)
        LOGGER.info("Chunks generated: %s", len(chunks))
        LOGGER.info("Time taken: %.2f seconds", elapsed)

        print("\nProcessing Summary")
        print(f"Files discovered : {len(files)}")
        print(f"Files processed  : {processed_files}")
        print(f"Files skipped    : {skipped_files}")
        print(f"Chunks generated : {len(chunks)}")
        print(f"Output saved to  : {self.config.output_file}")
        print(f"Time taken       : {elapsed:.2f} seconds")

        return chunks

    def _discover_files(self) -> list[Path]:
        if not self.config.input_dir.exists():
            raise FileNotFoundError(f"Input directory not found: {self.config.input_dir}")

        files = sorted(
            path
            for path in self.config.input_dir.iterdir()
            if path.is_file() and path.suffix.lower() in {".txt", ".pdf"}
        )

        if not files:
            LOGGER.warning("No .txt or .pdf files found in %s", self.config.input_dir)

        return files

    def _read_file(self, file_path: Path) -> str:
        if file_path.suffix.lower() == ".txt":
            return file_path.read_text(encoding="utf-8", errors="ignore")
        if file_path.suffix.lower() == ".pdf":
            return self._read_pdf(file_path)
        raise ValueError(f"Unsupported file type: {file_path.suffix}")

    def _read_pdf(self, file_path: Path) -> str:
        if PdfReader is None:
            raise RuntimeError("PDF support requires the optional 'pypdf' package.")

        reader = PdfReader(str(file_path))
        pages: list[str] = []
        for index, page in enumerate(reader.pages, start=1):
            try:
                text = page.extract_text() or ""
            except Exception as exc:  # pragma: no cover - defensive logging
                LOGGER.warning("Failed to extract text from %s page %s: %s", file_path.name, index, exc)
                continue
            pages.append(text)
        return "\n\n".join(pages)

    def _clean_text(self, text: str) -> str:
        normalized = text.replace("\r\n", "\n").replace("\r", "\n")
        normalized = normalized.replace("\u00a0", " ")
        normalized = re.sub(r"[ \t]+", " ", normalized)
        normalized = re.sub(r"\n{3,}", "\n\n", normalized)
        normalized = re.sub(r"[^\S\n]+", " ", normalized)
        normalized = re.sub(r"[%&]{3,}", "", normalized)
        normalized = re.sub(r"[^\w\s.,;:!?()'\"/%&-]", "", normalized)
        normalized = re.sub(r"\.{4,}", "...", normalized)
        normalized = re.sub(r"([!?])\1{3,}", r"\1\1\1", normalized)
        normalized = re.sub(r"[ \t]+", " ", normalized)
        normalized = re.sub(r" ?\n ?", "\n", normalized)
        return normalized.strip()

    def _chunk_document(self, text: str, file_name: str) -> list[dict]:
        paragraph_matches = list(re.finditer(r"\S[\s\S]*?(?=\n{2,}\S|\Z)", text))
        if not paragraph_matches:
            return []

        chunks: list[dict] = []
        chunk_id = 1
        current_parts: list[dict] = []
        current_length = 0

        for match in paragraph_matches:
            paragraph_text = match.group(0).strip()
            if not paragraph_text:
                continue

            paragraph_span = {
                "text": paragraph_text,
                "start": match.start(),
                "end": match.end(),
            }
            paragraph_length = len(paragraph_text)

            if paragraph_length > self.config.max_chunk_chars:
                if current_parts:
                    chunks.append(self._build_chunk(file_name, chunk_id, current_parts))
                    chunk_id += 1
                    current_parts, current_length = self._overlap_parts(current_parts)

                for sentence_group in self._split_large_paragraph(paragraph_span):
                    group_length = len(sentence_group["text"])
                    if current_parts and current_length + group_length > self.config.max_chunk_chars:
                        chunks.append(self._build_chunk(file_name, chunk_id, current_parts))
                        chunk_id += 1
                        current_parts, current_length = self._overlap_parts(current_parts)
                    current_parts.append(sentence_group)
                    current_length += group_length
                continue

            should_flush = (
                current_parts
                and current_length >= self.config.min_chunk_chars
                and current_length + paragraph_length > self.config.max_chunk_chars
            )
            if should_flush:
                chunks.append(self._build_chunk(file_name, chunk_id, current_parts))
                chunk_id += 1
                current_parts, current_length = self._overlap_parts(current_parts)

            current_parts.append(paragraph_span)
            current_length += paragraph_length

        if current_parts:
            chunks.append(self._build_chunk(file_name, chunk_id, current_parts))

        return chunks

    def _split_large_paragraph(self, paragraph_span: dict) -> list[dict]:
        text = paragraph_span["text"]
        base_start = paragraph_span["start"]
        sentence_pattern = re.compile(r"[^.!?]+[.!?]?(?=\s+|$)", re.MULTILINE)
        sentence_matches = list(sentence_pattern.finditer(text))

        if not sentence_matches:
            return [paragraph_span]

        groups: list[dict] = []
        current_sentences: list[str] = []
        group_start = None
        running_length = 0

        for match in sentence_matches:
            sentence = match.group(0).strip()
            if not sentence:
                continue

            if group_start is None:
                group_start = base_start + match.start()

            projected_length = running_length + len(sentence) + (1 if current_sentences else 0)
            if current_sentences and projected_length > self.config.max_chunk_chars:
                combined = " ".join(current_sentences)
                groups.append(
                    {
                        "text": combined,
                        "start": group_start,
                        "end": group_start + len(combined),
                    }
                )
                current_sentences = [sentence]
                group_start = base_start + match.start()
                running_length = len(sentence)
            else:
                current_sentences.append(sentence)
                running_length = projected_length

        if current_sentences and group_start is not None:
            combined = " ".join(current_sentences)
            groups.append(
                {
                    "text": combined,
                    "start": group_start,
                    "end": group_start + len(combined),
                }
            )

        return groups or [paragraph_span]

    def _build_chunk(self, file_name: str, chunk_id: int, parts: Iterable[dict]) -> dict:
        parts_list = list(parts)
        chunk_text = "\n\n".join(part["text"] for part in parts_list).strip()
        start_char = min(part["start"] for part in parts_list)
        end_char = max(part["end"] for part in parts_list)
        return {
            "file_name": file_name,
            "chunk_id": chunk_id,
            "chunk_text": chunk_text,
            "length": len(chunk_text),
            "chunk_length": len(chunk_text),
            "start_char": start_char,
            "end_char": end_char,
        }

    def _overlap_parts(self, parts: list[dict]) -> tuple[list[dict], int]:
        if self.config.overlap_chars <= 0 or not parts:
            return [], 0

        retained: list[dict] = []
        retained_length = 0

        for part in reversed(parts):
            part_length = len(part["text"])
            if retained and retained_length + part_length > self.config.overlap_chars:
                break
            retained.insert(0, part)
            retained_length += part_length

        return retained, retained_length

    def _write_output(self, chunks: list[dict]) -> None:
        self.config.output_file.parent.mkdir(parents=True, exist_ok=True)

        if self.config.output_format == "json":
            with self.config.output_file.open("w", encoding="utf-8") as handle:
                json.dump(chunks, handle, indent=2, ensure_ascii=False)
            return

        if self.config.output_format == "csv":
            fieldnames = [
                "file_name",
                "chunk_id",
                "chunk_text",
                "length",
                "chunk_length",
                "start_char",
                "end_char",
            ]
            with self.config.output_file.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(chunks)
            return

        raise ValueError("output_format must be either 'json' or 'csv'")
