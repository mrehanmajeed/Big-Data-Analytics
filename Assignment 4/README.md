# Big Data Analytics — Assignment 4

Project: Big Data Analytics — Assignment 4

## Overview
This repository contains the code and configuration for Assignment 4 of the Big Data Analytics course. The project is a small Python application that demonstrates data processing and testing practices. It includes a main application file, unit tests, and Docker configuration to run the app in a container.

## Project aims
- Provide a reproducible example of a simple data-processing Python program.
- Show how to package and run the application using Docker and Docker Compose.
- Demonstrate unit testing using the provided `test_main.py`.
- Serve as a template for coursework: clear structure, run/test instructions, and minimal configuration.

## Repository structure

```
Assignment 4/
├─ Dockerfile               # Dockerfile to build the application image
├─ docker-compose.yaml      # Docker Compose configuration to run the app
├─ main.py                  # Main application entrypoint
├─ test_main.py             # Unit tests for the main logic
└─ README.md                # This file
```

> Note: The structure above shows the files located at the root of the `Assignment 4` workspace.

## Requirements
- Python 3.8+ (recommended)
- Docker & Docker Compose (optional, for containerized runs)

## How to run locally
1. Create and activate a virtual environment (recommended):

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
```

2. Install dependencies (if any). If the project has no `requirements.txt`, you can run the script directly.

```powershell
pip install -r requirements.txt  # optional
python main.py
```

## Run with Docker
Build the Docker image and run the container:

```powershell
docker build -t assignment4:latest .
docker run --rm assignment4:latest
```

Or use Docker Compose:

```powershell
docker-compose up --build
```

## Testing
Run unit tests with pytest:

```powershell
pip install pytest  # if needed
pytest -q
```

## Notes & assumptions
- This README assumes `main.py` is a small, self-contained script and `test_main.py` contains pytest-compatible tests.
- If you want a `requirements.txt`, add your dependencies and re-run the install step.

## Next steps (optional enhancements)
- Add a `requirements.txt` listing exact dependencies.
- Improve `Dockerfile` with pinned Python base image and non-root user.
- Add CI workflow to run tests on push (GitHub Actions or similar).

---

If you'd like, I can also:
- Add `requirements.txt` with common data packages (pandas, numpy, pytest).
- Inspect `main.py` and `test_main.py` and update README to document specific entrypoints and examples.

Tell me which of those you'd like me to do next.