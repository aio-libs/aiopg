import unittest

from aiopg import _parse_version


class VersionTests(unittest.TestCase):

    def test_alpha(self):
        self.assertEqual((0, 1, 2, 'alpha', 2), _parse_version('0.1.2a2'))
        self.assertEqual((1, 2, 3, 'alpha', 0), _parse_version('1.2.3a'))

    def test_beta(self):
        self.assertEqual((0, 1, 2, 'beta', 2), _parse_version('0.1.2b2'))
        self.assertEqual((0, 1, 2, 'beta', 0), _parse_version('0.1.2b'))

    def test_rc(self):
        self.assertEqual((0, 1, 2, 'candidate', 5), _parse_version('0.1.2c5'))
        self.assertEqual((0, 1, 2, 'candidate', 0), _parse_version('0.1.2c'))

    def test_final(self):
        self.assertEqual((0, 1, 2, 'final', 0), _parse_version('0.1.2'))

    def test_invalid(self):
        self.assertRaises(ImportError, _parse_version, '0.1')
        self.assertRaises(ImportError, _parse_version, '0.1.1.2')
        self.assertRaises(ImportError, _parse_version, '0.1.1z2')
