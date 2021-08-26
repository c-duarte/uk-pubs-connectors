import unittest

import pandas

from uk_pubs_connectors.admiral_taverns import AdmiralTavernsConnector


class TestRawData(unittest.TestCase):
    data: pandas.DataFrame

    def setUp(self) -> None:
        connector = AdmiralTavernsConnector()
        self.data = connector.get()

    def test_null(self) -> None:
        null_data = self.data.loc[self.data.isna().any(axis=1)]\
            .to_dict('records')

        self.assertListEqual(null_data, [])

    def test_approximate_price(self) -> None:
        invalid = self.data.loc[
            ~self.data['ApproximatePrice']
            .str.match(r'Approximate Ingoings [£]?[\d,]+'),
            ['ApproximatePrice']
        ]
        invalid = invalid.reset_index().to_dict('records')

        self.assertListEqual(invalid, [])


class TestTransformed(unittest.TestCase):
    data: pandas.DataFrame

    def setUp(self) -> None:
        connector = AdmiralTavernsConnector()
        self.data = connector.clean(connector.get())
        self.data.to_csv('env\\foo.csv', index=False)

    def test_values(self) -> None:
        print(self.data['AnnualRent'].max())
        print(self.data['AnnualRent'].min())
