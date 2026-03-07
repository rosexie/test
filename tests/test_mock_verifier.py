import subprocess
import sys
import unittest


class MockVerifierScriptTest(unittest.TestCase):
    def test_mock_verifier_passes(self):
        proc = subprocess.run(
            [sys.executable, 'scripts/verify_dashboard_with_mock.py'],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stdout + proc.stderr)
        self.assertIn('[PASS] mock verification succeeded', proc.stdout)


if __name__ == '__main__':
    unittest.main()
