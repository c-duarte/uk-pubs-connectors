import unittest

from uk_pubs_connectors.admiraltaverns import AdmiralTavernsConnector


class TestAdmiralTaverns(unittest.TestCase):
    def test(self) -> None:
        connector = AdmiralTavernsConnector()
        connector.get()