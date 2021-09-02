import html
import re

import pandas

from uk_pubs.greene_king.constants import BASE_URL


class GreeneKingDataProcessor:
    '''Treats data from Greene King Pubs'''

    def process(self, input: pandas.DataFrame) -> pandas.DataFrame:
        '''Clean and transform raw data about Pubs from the Greene King source.

        :param input: Raw data from Greene King website
        :type input: pandas.DataFrame
        :return: Clean and transformed data
        :rtype: pandas.DataFrame
        '''
        output = input.copy()
        output.fillna('', inplace=True)
        output['Name'] = output['properties.data.field_pub_name_only']\
            .apply(html.unescape)
        output['URL'] = BASE_URL + output['properties.data.title']\
            .str.extract(r'href="([^"]+)"')
        output['StreetAddress'] = (
            output['properties.data.address_line1']
            + ' ' + output['properties.data.locality']
            + ' ' + output['properties.data.postal_code']
        ).str.replace(r' +', ' ', regex=True)
        output.replace(
            {
                'properties.data.field_agreement_annual_rent': 'AnnualRent'
            },
            inplace=True
        )
        output[['Long', 'Lat']] = output['geometry.coordinates']\
            .str.extract(r'\[([^,]+), ([^\]]+)\]')

        spare_columns = [
            column for column in output.columns
            if re.match(r'type|geometry|properties', column)
        ]

        output.drop(columns=spare_columns, inplace=True)

        return output
