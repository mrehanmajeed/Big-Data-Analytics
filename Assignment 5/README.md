# Paraquat CRUD with Optional HDFS Persistence

Summary
-------
This small Python project implements a simple CRUD (Create, Read, Update, Delete) CLI for storing records about paraquat chemical samples. Data is stored as a Parquet file (`paraquat_data.parquet`) in the project root. The project also supports optional persistence to HDFS via WebHDFS (using the `hdfs` Python package). If HDFS is not available or not configured, the application writes copies and an append-only creation audit log to a local fallback directory `hdfs_fallback/`.

Key features
- CLI menu for creating, reading, updating, and deleting entries.
- Local storage using Parquet (fast, columnar format) with `pandas` and `pyarrow`.
- Optional HDFS upload of the Parquet file and append-only creation log when `HDFS_URL` is provided and the `hdfs` package is installed.
- Safe local fallback when HDFS isn't reachable.

Project structure
-----------------
```
Assignment 5/
├─ app.py                # Main application and HDFS sync logic
├─ test_app.py           # Unit test verifying Parquet + fallback log behaviour
├─ requirements.txt      # Python dependencies (pandas, pyarrow, hdfs)
├─ paraquat_data.parquet # (created at runtime) primary data store
└─ hdfs_fallback/        # created at runtime if HDFS is not used; contains copies and logs
   ├─ paraquat_data.parquet
   └─ creations.log
```

How it works (brief)
--------------------
- The application ensures a local Parquet file `paraquat_data.parquet` exists and uses it as the single source of truth.
- On `create_entry(...)`, the record is added to the Parquet file.
- After saving locally, the application attempts to upload the Parquet file to HDFS and append a JSON line to `creations.log` in HDFS.
- If HDFS is not configured or the `hdfs` package isn't installed, the file and the creation log are written under `hdfs_fallback/` in the project directory.

Configuration (HDFS)
--------------------
Set the following environment variables to enable HDFS uploads:
- `HDFS_URL` — e.g. `http://namenode:9870` (required to enable HDFS use)
- `HDFS_USER` — user name to use with HDFS (optional; defaults to `hdfs`)
- `HDFS_DIR` — directory on HDFS to place files (optional; defaults to `/user/<HDFS_USER>`)

Example (PowerShell):
```powershell
$env:HDFS_URL = 'http://namenode:9870'
$env:HDFS_USER = 'myuser'
python .\app.py
```

Install dependencies
--------------------
(Optionally create a virtual environment first.)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r .\requirements.txt
```

Run the app
-----------
```powershell
python .\app.py
```
Choose menu option 1 to create an entry; other options allow reading, updating and deleting records.

Run tests
---------
The repository includes a unit test `test_app.py` which verifies that creating an entry writes the Parquet file and a creation log into the fallback directory (used when HDFS isn't configured).

```powershell
python -m unittest -v
```

Notes and limitations
---------------------
- This project uses the `hdfs` Python package's `InsecureClient` to interact with WebHDFS; secure/kerberos setups require additional configuration.
- The HDFS append logic reads the existing log then re-uploads it. This is intentionally simple and safe for environments that do not support native append.
- For production, consider transaction/locking strategies if concurrent writers are expected.

Credits
-------
Created for an assignment to demonstrate local persistence, simple HDFS integration, and testability.
