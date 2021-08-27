import html

import pandas

from uk_pubs.punch_pubs.constants import NAME


class PunchPubsDataProcessor:
    '''Treats data from Punch Pubs'''

    def process(self, input: pandas.DataFrame) -> pandas.DataFrame:
        '''Clean and transform raw data about Pubs from the Punch Pubs source.

        :param input: Raw data from Punch Pubs website
        :type input: pandas.DataFrame
        :return: Clean and transformed data
        :rtype: pandas.DataFrame
        '''
        output = pandas.DataFrame()
        output = input.loc[:, [
            'outgoing_value',
            'name',
            'permalink',
            'latlng.lat',
            'latlng.lng',
            'address',
            'town',
        ]]
        output['outgoing_value'] = output['outgoing_value']\
            .str.replace(r'[ ,]', '', regex=True)\
            .str.upper()

        weekly_rent = output['outgoing_value']\
            .str.extract(r'£([\d,]+)PW$')\
            .astype(float)\
            * 52
        monthly_rent = output['outgoing_value']\
            .str.extract(r'£([\d,]+)PCM$')\
            .astype(float)\
            * 12
        annual_rent = output['outgoing_value']\
            .str.extract(r'£([\d,]+)$')\
            .astype(float)

        output['outgoing_value'] = annual_rent\
            .combine_first(monthly_rent)\
            .combine_first(weekly_rent)

        output.rename(columns={
            'outgoing_value': 'AnnualRent',
            'name': 'Name',
            'permalink': 'URL',
            'latlng.lat': 'Lat',
            'latlng.lng': 'Long',
            'address': 'FormattedAddress',
            'town': 'City',
        }, inplace=True)

        output['Name'] = output['Name'].apply(html.unescape)

        output['Source'] = NAME

        return output
