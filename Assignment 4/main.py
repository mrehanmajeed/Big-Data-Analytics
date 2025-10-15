import os
from hdfs import InsecureClient

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class HadoopCRUD:
    """Simple CRUD helper around HDFS using WebHDFS."""

    def __init__(self):
        self.hdfs_host = os.getenv("HDFS_HOST", "localhost")
        self.hdfs_port = os.getenv("HDFS_PORT", "9870")
        self.hdfs_user = os.getenv("HDFS_USER", "hdfs")
        self.base_url = f"http://{self.hdfs_host}:{self.hdfs_port}"
        self.client = InsecureClient(self.base_url, user=self.hdfs_user)
        print(f"Connected to HDFS at {self.base_url} as {self.hdfs_user}")

    def create_file(self, hdfs_path, local_file_path=None, data=None):
        try:
            if local_file_path:
                self.client.upload(hdfs_path, local_file_path, overwrite=True)
                print(f"File uploaded: {local_file_path} -> {hdfs_path}")
            elif data is not None:
                with self.client.write(hdfs_path, encoding="utf-8", overwrite=True) as writer:
                    writer.write(data)
                print(f"Data written to: {hdfs_path}")
            else:
                print("Either local_file_path or data must be provided")
                return False
            return True
        except Exception as exc:
            print(f"Error creating file: {exc}")
            return False

    def read_file(self, hdfs_path, download_to=None):
        try:
            if download_to:
                self.client.download(hdfs_path, download_to, overwrite=True)
                print(f"File downloaded: {hdfs_path} -> {download_to}")
                return download_to
            with self.client.read(hdfs_path, encoding="utf-8") as reader:
                content = reader.read()
            print(f"File read: {hdfs_path}")
            return content
        except Exception as exc:
            print(f"Error reading file: {exc}")
            return None

    def update_file(self, hdfs_path, local_file_path=None, data=None):
        try:
            if not self.file_exists(hdfs_path):
                print(f"File does not exist: {hdfs_path}")
                return False
            return self.create_file(hdfs_path, local_file_path, data)
        except Exception as exc:
            print(f"Error updating file: {exc}")
            return False

    def delete_file(self, hdfs_path, recursive=False):
        try:
            self.client.delete(hdfs_path, recursive=recursive)
            print(f"Deleted: {hdfs_path}")
            return True
        except Exception as exc:
            print(f"Error deleting file: {exc}")
            return False

    def list_files(self, hdfs_path="/"):
        try:
            files = self.client.list(hdfs_path)
            print(f"Files in {hdfs_path}:")
            for entry in files:
                print(f"  - {entry}")
            return files
        except Exception as exc:
            print(f"Error listing files: {exc}")
            return []

    def file_exists(self, hdfs_path):
        try:
            status = self.client.status(hdfs_path, strict=False)
            return status is not None
        except Exception:
            return False

    def create_directory(self, hdfs_path):
        try:
            self.client.makedirs(hdfs_path)
            print(f"Directory created: {hdfs_path}")
            return True
        except Exception as exc:
            print(f"Error creating directory: {exc}")
            return False

    def get_file_info(self, hdfs_path):
        try:
            info = self.client.status(hdfs_path)
            print(f"File info for {hdfs_path}:")
            for key, value in info.items():
                print(f"  {key}: {value}")
            return info
        except Exception as exc:
            print(f"Error getting file info: {exc}")
            return None


def main():
    print("Hadoop HDFS CRUD Operations")
    hadoop = HadoopCRUD()

    menu = {
        "1": "Create File",
        "2": "Read File",
        "3": "Update File",
        "4": "Delete File",
        "5": "List Files",
        "6": "Create Directory",
        "7": "Get File Info",
        "8": "Exit",
    }

    while True:
        for key, label in menu.items():
            print(f"{key}. {label}")
        choice = input("Enter choice: ").strip()

        if choice == "1":
            hdfs_path = input("HDFS path: ").strip()
            method = input("Upload file (1) or write text (2): ").strip()
            if method == "1":
                local_path = input("Local file path: ").strip()
                hadoop.create_file(hdfs_path, local_file_path=local_path)
            elif method == "2":
                data = input("Text data: ")
                hadoop.create_file(hdfs_path, data=data)
            else:
                print("Invalid option")

        elif choice == "2":
            hdfs_path = input("HDFS path: ").strip()
            method = input("Download (1) or view (2): ").strip()
            if method == "1":
                local_path = input("Local path: ").strip()
                hadoop.read_file(hdfs_path, download_to=local_path)
            elif method == "2":
                content = hadoop.read_file(hdfs_path)
                if content is not None:
                    print(content)
            else:
                print("Invalid option")

        elif choice == "3":
            hdfs_path = input("HDFS path: ").strip()
            method = input("Upload file (1) or write text (2): ").strip()
            if method == "1":
                local_path = input("Local file path: ").strip()
                hadoop.update_file(hdfs_path, local_file_path=local_path)
            elif method == "2":
                data = input("Text data: ")
                hadoop.update_file(hdfs_path, data=data)
            else:
                print("Invalid option")

        elif choice == "4":
            hdfs_path = input("HDFS path: ").strip()
            is_dir = input("Directory? (y/n): ").strip().lower() == "y"
            hadoop.delete_file(hdfs_path, recursive=is_dir)

        elif choice == "5":
            hdfs_path = input("Directory path (default /): ").strip() or "/"
            hadoop.list_files(hdfs_path)

        elif choice == "6":
            hdfs_path = input("Directory path: ").strip()
            hadoop.create_directory(hdfs_path)

        elif choice == "7":
            hdfs_path = input("HDFS path: ").strip()
            hadoop.get_file_info(hdfs_path)

        elif choice == "8":
            print("Goodbye")
            break

        else:
            print("Invalid choice")


if __name__ == "__main__":
    main()


