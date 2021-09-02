import pandas


class StonegateDataProcessor:
    '''Treats data from Stonegate'''

    def process(self, input: pandas.DataFrame) -> pandas.DataFrame:
        '''Clean and transform raw data about Pubs from the Stonegate source.

        :param input: Raw data from Stonegate website
        :type input: pandas.DataFrame
        :return: Clean and transformed data
        :rtype: pandas.DataFrame
        '''
        output = pandas.DataFrame()

        output = input.loc[:, [
            'GuideRent',
            'PubName',
            'PubLinkUrl',
            'Latitude',
            'Longitude',
            'PubAddress',
            'Postcode',
            'ScrapeDate',
            'Source'
        ]]

        output.rename(columns={
            'GuideRent': 'AnnualRent',
            'PubName': 'Name',
            'PubLinkUrl': 'URL',
            'Latitude': 'Lat',
            'Longitude': 'Long',
            'PubAddress': 'StreetAddress',
            'Postcode': 'PostalCode',
        }, inplace=True)

        return output
