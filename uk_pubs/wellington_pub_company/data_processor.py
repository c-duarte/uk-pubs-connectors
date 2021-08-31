import logging
import pandas


class WellingtonPubCompanyDataProcessor:
    '''Treats data from Wellington Pub Company'''

    def process(self, input: pandas.DataFrame) -> pandas.DataFrame:
        '''Clean and transform raw data about Pubs from the Wellington Pub
        Company source.

        :param input: Raw data from Wellington Pub Company website
        :type input: pandas.DataFrame
        :return: Clean and transformed data
        :rtype: pandas.DataFrame
        '''
        output = input.copy()
        output.drop(
            columns=['ID', 'Tags', 'Description', 'NegotiatorMailURL'],
            inplace=True
        )

        output[['Name', 'Address']] = output['NameAddress']\
            .str.extract(r'^([^,]+), (.+)$')

        output.drop(columns=['NameAddress'], inplace=True)

        return output


if __name__ == '__main__':
    from pprint import pprint
    from uk_pubs.wellington_pub_company.connector import \
        WellingtonPubCompanyConnector

    con = WellingtonPubCompanyConnector()
    data = con.get()

    data_processor = WellingtonPubCompanyDataProcessor()

    data = data_processor.process(data)

    pprint(data)
