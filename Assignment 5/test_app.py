import os
import sys
import unittest
import tempfile
import shutil
import json
from importlib import reload

# We'll import app after adjusting sys.path to the project dir
PROJECT_DIR = os.path.dirname(__file__)

class TestAppPersistence(unittest.TestCase):
    def setUp(self):
        # create a temp dir and copy app.py and requirements (if any)
        self.orig_cwd = os.getcwd()
        self.tmpdir = tempfile.mkdtemp()
        # copy project files
        shutil.copy(os.path.join(PROJECT_DIR, 'app.py'), self.tmpdir)
        # change cwd to tmpdir
        os.chdir(self.tmpdir)
        sys.path.insert(0, self.tmpdir)
        # ensure fallback dir removed before importing so module can create it
        if os.path.exists(os.path.join(self.tmpdir, 'hdfs_fallback')):
            shutil.rmtree(os.path.join(self.tmpdir, 'hdfs_fallback'))
        # import the app module freshly
        import app
        reload(app)
        self.app = app

    def tearDown(self):
        # cleanup
        os.chdir(self.orig_cwd)
        sys.path = [p for p in sys.path if p != self.tmpdir]
        shutil.rmtree(self.tmpdir)

    def test_create_writes_parquet_and_log(self):
        # create a sample entry
        entry = {
            'chemical_name': 'ParaquatTest',
            'concentration': '5%',
            'location': 'Lab A',
            'date': '2025-10-15'
        }
        # call create_entry
        self.app.create_entry(entry)
        # check parquet exists
        self.assertTrue(os.path.exists('paraquat_data.parquet'))
        # check fallback log exists and has our entry
        fallback_log = os.path.join('hdfs_fallback', 'creations.log')
        self.assertTrue(os.path.exists(fallback_log))
        # read log and ensure contains chemical_name
        with open(fallback_log, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        self.assertGreaterEqual(len(lines), 1)
        found = any('ParaquatTest' in line for line in lines)
        self.assertTrue(found)

if __name__ == '__main__':
    unittest.main()
