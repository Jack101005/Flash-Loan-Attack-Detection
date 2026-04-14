# Flash-Loan Attack Backend (Modern UV Setup)

A ultra-fast FastAPI backend service for the Flash-Loan Attack Detection system.

## Prerequisites
- **Python 3.9+** installed
- **[uv](https://docs.astral.sh/uv/)** package manager installed

## 1. Quick Setup

The backend dependencies and environment are entirely governed by `uv`, utilizing `pyproject.toml` and `uv.lock`.

Install dependencies and implicitly create the virtual environment:

```bash
uv sync
```

## 2. Run the Development Server

To start the FastAPI server with hot-reloading enabled, simply run:

```bash
uv run uvicorn Main:app --reload --port 8000
```
*Note: You do not need to manually activate any virtual environment. `uv run` handles the path injection dynamically!*

The server will start and listen on `http://127.0.0.1:8000`.

## 3. Interactive API Documentation

FastAPI provides auto-generated documentation accessible in your browser:
- **Swagger UI**: `http://127.0.0.1:8000/docs`
- **ReDoc**: `http://127.0.0.1:8000/redoc`

## 4. Managing Dependencies

```bash
# Install a new package
uv add <package-name>

# Remove a package
uv remove <package-name>
```

## Project Structure
```
backend/
├── Main.py                # FastAPI Application Entry
├── pyproject.toml         # Modern Project Metadata & Config
├── uv.lock                # Deterministic Dependency Graph
└── README.md              # This file
```
