# Smart Document Ingestion & Semantic Chunking

Standalone Python project for ingesting `.txt` and `.pdf` files, cleaning the text, chunking by semantic boundaries, and exporting chunk metadata to JSON or CSV.

## Project Structure

- `src/smart_doc_ingestion/`: source package
- `sample_input/`: sample text files for a quick run
- `output/`: generated chunk exports
- `tests/`: lightweight tests

## Requirements

- Python 3.10+
- Optional PDF support: `pypdf`

## Install

```bash
pip install -r requirements.txt
```

For a fresh checkout, either install the package in editable mode or run the helper launcher from the repo root.

```bash
pip install -e .
```

## Run

```bash
python -m smart_doc_ingestion.cli --input-dir sample_input --output-file output/chunks.json
```

If you have not installed the package, run from the repository root with:

```bash
python run_ingestion.py --input-dir sample_input --output-file output/chunks.json
```

On Windows PowerShell with a local virtual environment, that is typically:

```powershell
.\.venv\Scripts\python.exe run_ingestion.py --input-dir sample_input --output-file output/chunks.json
```

## CSV Output

```bash
python -m smart_doc_ingestion.cli --input-dir sample_input --output-file output/chunks.csv --output-format csv
```

## Text Cleaning

The text cleaning logic lives in `src/smart_doc_ingestion/pipeline.py` inside `_clean_text()`. It currently:

- normalizes Windows line endings
- replaces non-breaking spaces
- collapses repeated spaces and tabs
- reduces 3+ blank lines to 2
- removes most non-word special characters while keeping common punctuation
- strips repeated symbol noise such as `%%%` or `@@@` while preserving normal `%` and `&` usage
- trims spaces around newlines
