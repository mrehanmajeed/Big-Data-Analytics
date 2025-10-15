import os
import importlib
import unittest
from unittest.mock import MagicMock, patch


class HadoopCRUDTestCase(unittest.TestCase):
    def setUp(self):
        self.env_patch = patch.dict(
            os.environ,
            {"HDFS_HOST": "namenode", "HDFS_PORT": "50070", "HDFS_USER": "tester"},
            clear=True,
        )
        self.env_patch.start()
        import main as main_module

        self.main = importlib.reload(main_module)
        self.client_patcher = patch.object(self.main, "InsecureClient")
        self.mock_client_class = self.client_patcher.start()
        self.mock_client = MagicMock()
        self.mock_client_class.return_value = self.mock_client
        self.mock_writer = MagicMock()
        self.mock_client.write.return_value.__enter__.return_value = self.mock_writer
        self.mock_reader = MagicMock()
        self.mock_reader.read.return_value = "content"
        self.mock_client.read.return_value.__enter__.return_value = self.mock_reader
        self.crud = self.main.HadoopCRUD()

    def tearDown(self):
        self.client_patcher.stop()
        self.env_patch.stop()

    def test_create_file_with_data(self):
        result = self.crud.create_file("/path/file.txt", data="hello")
        self.assertTrue(result)
        self.mock_client.write.assert_called_once_with("/path/file.txt", encoding="utf-8", overwrite=True)
        self.mock_writer.write.assert_called_once_with("hello")

    def test_create_file_from_local_path(self):
        result = self.crud.create_file("/path/file.txt", local_file_path="local.txt")
        self.assertTrue(result)
        self.mock_client.upload.assert_called_once_with("/path/file.txt", "local.txt", overwrite=True)

    def test_create_file_requires_input(self):
        result = self.crud.create_file("/path/file.txt")
        self.assertFalse(result)

    def test_read_file_as_text(self):
        response = self.crud.read_file("/path/file.txt")
        self.assertEqual(response, "content")
        self.mock_client.read.assert_called_once_with("/path/file.txt", encoding="utf-8")

    def test_read_file_download(self):
        response = self.crud.read_file("/path/file.txt", download_to="out.txt")
        self.assertEqual(response, "out.txt")
        self.mock_client.download.assert_called_once_with("/path/file.txt", "out.txt", overwrite=True)

    def test_update_file_missing(self):
        self.mock_client.status.return_value = None
        result = self.crud.update_file("/path/file.txt", data="new")
        self.assertFalse(result)

    def test_update_file_existing(self):
        self.mock_client.status.return_value = {"type": "FILE"}
        result = self.crud.update_file("/path/file.txt", data="new")
        self.assertTrue(result)
        self.mock_client.write.assert_called_with("/path/file.txt", encoding="utf-8", overwrite=True)

    def test_delete_file(self):
        result = self.crud.delete_file("/path/file.txt")
        self.assertTrue(result)
        self.mock_client.delete.assert_called_once_with("/path/file.txt", recursive=False)

    def test_delete_directory_recursive(self):
        result = self.crud.delete_file("/dir", recursive=True)
        self.assertTrue(result)
        self.mock_client.delete.assert_called_once_with("/dir", recursive=True)

    def test_list_files(self):
        self.mock_client.list.return_value = ["a", "b"]
        files = self.crud.list_files("/dir")
        self.assertEqual(files, ["a", "b"])
        self.mock_client.list.assert_called_once_with("/dir")

    def test_file_exists(self):
        self.mock_client.status.return_value = {"type": "FILE"}
        self.assertTrue(self.crud.file_exists("/path/file.txt"))
        self.mock_client.status.assert_called_once_with("/path/file.txt", strict=False)

    def test_create_directory(self):
        result = self.crud.create_directory("/newdir")
        self.assertTrue(result)
        self.mock_client.makedirs.assert_called_once_with("/newdir")

    def test_get_file_info(self):
        self.mock_client.status.return_value = {"type": "FILE"}
        info = self.crud.get_file_info("/path/file.txt")
        self.assertEqual(info, {"type": "FILE"})
        self.mock_client.status.assert_called_with("/path/file.txt")


if __name__ == "__main__":
    unittest.main()
