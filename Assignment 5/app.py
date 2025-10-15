import pandas as pd
import os
import json
import logging
import tempfile

try:
    # hdfs client for WebHDFS (optional)
    from hdfs import InsecureClient
    HDFS_AVAILABLE = True
except Exception:
    InsecureClient = None
    HDFS_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Local data file (parquet)
DATA_FILE = "paraquat_data.parquet"

# HDFS configuration (use environment variables to configure)
HDFS_URL = os.environ.get('HDFS_URL')  # e.g. 'http://namenode:50070' or 'http://namenode:9870'
HDFS_USER = os.environ.get('HDFS_USER', 'hdfs')
HDFS_DIR = os.environ.get('HDFS_DIR', f"/user/{HDFS_USER}")
HDFS_DATA_PATH = os.path.join(HDFS_DIR, 'paraquat_data.parquet').replace('\\', '/')
HDFS_LOG_PATH = os.path.join(HDFS_DIR, 'creations.log').replace('\\', '/')

# Fallback directory when HDFS is not available or not configured
LOCAL_HDFS_FALLBACK_DIR = os.path.join(os.getcwd(), 'hdfs_fallback')
os.makedirs(LOCAL_HDFS_FALLBACK_DIR, exist_ok=True)

def ensure_local_db():
    if not os.path.exists(DATA_FILE):
        df = pd.DataFrame(columns=["id", "chemical_name", "concentration", "location", "date"])
        df.to_parquet(DATA_FILE, index=False)


def init_hdfs_client():
    """Initialize and return an HDFS client (or None).

    Client returned only if HDFS is configured and the hdfs package is installed.
    """
    if not HDFS_URL or not HDFS_AVAILABLE:
        return None
    try:
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        # quick check
        client.status('/', strict=False)
        logging.info('HDFS client initialized.')
        return client
    except Exception as e:
        logging.warning(f"HDFS client init failed: {e}")
        return None


def sync_to_hdfs(local_path, hdfs_path, client=None):
    """Upload a local file to HDFS (overwrite). If no HDFS, copy to fallback dir."""
    if client is None:
        # copy to fallback directory
        dest = os.path.join(LOCAL_HDFS_FALLBACK_DIR, os.path.basename(hdfs_path))
        try:
            import shutil
            shutil.copy2(local_path, dest)
            logging.info(f'Wrote to local fallback: {dest}')
        except Exception as e:
            logging.error(f'Failed to write fallback file: {e}')
        return

    try:
        # InsecureClient.upload wants the hdfs destination folder or file
        client.upload(hdfs_path, local_path, overwrite=True)
        logging.info(f'Uploaded {local_path} to HDFS:{hdfs_path}')
    except Exception as e:
        logging.warning(f'Failed to upload to HDFS: {e}. Writing to fallback.')
        sync_to_hdfs(local_path, hdfs_path, client=None)


def append_creation_log(entry: dict, client=None):
    """Append a JSON line for each creation to HDFS log file, or fallback locally."""
    line = json.dumps(entry, default=str) + "\n"
    if client is None:
        # append to local fallback log
        try:
            with open(os.path.join(LOCAL_HDFS_FALLBACK_DIR, os.path.basename(HDFS_LOG_PATH)), 'a', encoding='utf-8') as f:
                f.write(line)
            logging.info('Appended creation log to local fallback.')
        except Exception as e:
            logging.error(f'Failed to append fallback log: {e}')
        return

    try:
        # If log exists, read and append locally then overwrite via upload (safer for environments without append support)
        existing = b''
        try:
            existing = client.read(HDFS_LOG_PATH)
        except Exception:
            existing = b''

        new_content = existing.decode('utf-8') + line if existing else line
        # write via a temp file then upload
        with tempfile.NamedTemporaryFile('w', delete=False, encoding='utf-8') as tmp:
            tmp.write(new_content)
            tmp_path = tmp.name
        client.upload(HDFS_LOG_PATH, tmp_path, overwrite=True)
        os.remove(tmp_path)
        logging.info('Appended creation log to HDFS.')
    except Exception as e:
        logging.warning(f'Failed to append log to HDFS: {e}. Writing to fallback.')
        append_creation_log(entry, client=None)


ensure_local_db()


def load_data():
    return pd.read_parquet(DATA_FILE)


def save_data(df, sync_hdfs=True):
    df.to_parquet(DATA_FILE, index=False)
    # attempt to sync to HDFS (or fallback)
    client = init_hdfs_client()
    if sync_hdfs:
        sync_to_hdfs(DATA_FILE, HDFS_DATA_PATH, client=client)


def create_entry(entry):
    df = load_data()
    entry['id'] = int(df['id'].max() + 1) if not df.empty else 1
    df = pd.concat([df, pd.DataFrame([entry])], ignore_index=True)
    # save locally and attempt a sync
    save_data(df, sync_hdfs=True)
    # append a creation log (keeps a simple audit trail)
    client = init_hdfs_client()
    append_creation_log(entry, client=client)
    print("✅ Entry added successfully.")


def read_entries():
    df = load_data()
    print(df)


def update_entry(entry_id, updated_data):
    df = load_data()
    if entry_id in df['id'].values:
        for key, value in updated_data.items():
            df.loc[df['id'] == entry_id, key] = value
        save_data(df)
        print("✅ Entry updated.")
    else:
        print("❌ Entry not found.")


def delete_entry(entry_id):
    df = load_data()
    if entry_id in df['id'].values:
        df = df[df['id'] != entry_id]
        save_data(df)
        print("✅ Entry deleted.")
    else:
        print("❌ Entry not found.")


def menu():
    while True:
        print("\n--- Paraquat CRUD Menu ---")
        print("1. Create Entry")
        print("2. Read Entries")
        print("3. Update Entry")
        print("4. Delete Entry")
        print("5. Exit")

        choice = input("Choose an option: ")
        
        if choice == "1":
            chemical_name = input("Chemical Name: ")
            concentration = input("Concentration: ")
            location = input("Location: ")
            date = input("Date (YYYY-MM-DD): ")
            entry = {
                "chemical_name": chemical_name,
                "concentration": concentration,
                "location": location,
                "date": date
            }
            create_entry(entry)

        elif choice == "2":
            read_entries()

        elif choice == "3":
            try:
                entry_id = int(input("Enter ID to update: "))
                field = input("Field to update: ")
                value = input("New value: ")
                update_entry(entry_id, {field: value})
            except ValueError:
                print("❌ Invalid ID.")

        elif choice == "4":
            try:
                entry_id = int(input("Enter ID to delete: "))
                delete_entry(entry_id)
            except ValueError:
                print("❌ Invalid ID.")

        elif choice == "5":
            print("👋 Exiting.")
            break
        else:
            print("❌ Invalid choice.")


if __name__ == "__main__":
    menu()
