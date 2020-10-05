import re
import requests
from bs4 import BeautifulSoup
import pandas as pd


def calculate_stat_before_round(df, stats):
    return df[stats] - df[stats].diff()


def calculate_stat_in_round(df, stats):
    return df[stats].diff()


def func_in_player_year(self, features, func, **func_parameters):
    for year in self['year'].unique():
        year_df = self[self['year'] == year]

        for player in year_df['atletas.atleta_id'].unique():
            player_df = year_df[year_df['atletas.atleta_id'] == player]
            self.loc[player_df.index, features] = func(
                player_df, features, **func_parameters)

    return self


def read_cartola_data(year):
    '''
    Read data from a given year of the CaRtola repository

    Parameters:
    year (int) - year inside the range 2018-2020.
    '''

    if year in [2018, 2019, 2020]:

        url = 'https://github.com/henriquepgomide/caRtola/tree/master/data/{}'.format(
            year)
        html = requests.get(url)

        soup = BeautifulSoup(html.text, 'lxml')

        dict_of_files = {}
        for tag in soup.find_all('a', attrs={'href': re.compile('rodada-([0-9]|[0-9][0-9])\.csv')}):
            href_str = tag.get('href')
            file_name = re.sub('/henriquepgomide/caRtola/blob/master/data/{}/'.format(year),
                               '',
                               href_str)

            file_url = re.sub('/henriquepgomide/caRtola/blob/master/data/{}/'.format(year),
                              'https://raw.githubusercontent.com/henriquepgomide/caRtola/master/data/{}/'.format(
                                  year),
                              href_str)
            dict_of_files[file_name] = file_url

        list_of_dataframes = []
        for key, item in dict_of_files.items():
            df = pd.read_csv(item)
            df['rodada'] = key
            list_of_dataframes.append(df)

        df_cartola = pd.concat(list_of_dataframes)

        return df_cartola

    else:
        print('You need to add an year within the range: 2018 and 2020')
        return False


def load_raw(years=[2018, 2019, 2020]):
    dfs = []
    for year in years:
        df_year = read_cartola_data(year)
        df_year['year'] = year
        dfs.append(df_year)

    df = pd.concat(dfs, ignore_index=True)
    return df


def minimal_clean(df, end={'year': 2020, 'round': 12}):
    df.drop(columns=['Unnamed: 0'], inplace=True)
    df.drop(df[df['atletas.clube.id.full.name'] ==
               'atletas.clube.id.full.name'].index, inplace=True)

    NUMERIC_COLS = ['atletas.atleta_id', 'atletas.rodada_id', 'atletas.pontos_num', 'atletas.preco_num',
                    'atletas.variacao_num', 'FC', 'FD', 'FF', 'FS', 'G', 'I', 'RB', 'CA', 'PE',
                    'A', 'SG', 'DD', 'FT', 'GS', 'CV', 'GC', 'DP', 'PP', 'PI', 'DS']

    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col])

    df.sort_values(by=['year', 'atletas.rodada_id'], inplace=True)
    df.drop(df[(df['atletas.rodada_id'] >= end['round']) & (
        df['year'] >= end['year'])].index, inplace=True)

    df.drop(df[df['atletas.clube_id'].isna()].index, inplace=True)
    df.fillna(0, inplace=True)
    events = ['FC', 'FD', 'FF', 'FS', 'G', 'I', 'RB', 'CA', 'PE',
              'A', 'SG', 'DD', 'FT', 'GS', 'CV', 'GC', 'DP', 'PP', 'PI', 'DS']
    df.func_in_player_year(events, calculate_stat_before_round)
    df['events_count'] = df[events].sum(axis=1)
    df.fillna(0, inplace=True)
    df['events_round'] = df['events_count']
    df.func_in_player_year(['events_round'], calculate_stat_in_round)
    df.fillna(0, inplace=True)
    df.drop(df[(df['events_round'] == 0)].index, inplace=True)
    df.drop(columns=['events_round', 'events_count'])
    return df


pd.core.frame.DataFrame.func_in_player_year = func_in_player_year

if __name__ == "__main__":
    df = minimal_clean(load_raw())
    df.to_csv('data/raw/cartola.csv', index=False)
