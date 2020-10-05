def player_until_round(row, df):
    year_serie = df['year'] == row['year']
    player_serie = df['atletas.atleta_id'] == row['atletas.atleta_id']
    round_serie = df['atletas.rodada_id'] <= row['atletas.rodada_id']
    return df[year_serie & player_serie & round_serie].sort_values(by=['atletas.rodada_id'])


def count_games(row, df):
    return player_until_round(row, df).shape[0]


def calculate_stat(row, df, stat, method='main'):
    useful_df = player_until_round(row, df)
    if useful_df.shape[0] == 0:
        return 0

    if method == 'main':
        return useful_df[stat].mean()

    return None
