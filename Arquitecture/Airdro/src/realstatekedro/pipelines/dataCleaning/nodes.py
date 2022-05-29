import pandas as pd

def cleaningQuintoAndar(df: pd.DataFrame) -> pd.DataFrame:
    df['C0'] = df['C0'].apply(lambda x: 0 if(x == 'vazio') else 1)
    df['C1'] = df['C1'].apply(lambda x: 'Desconhecido' if (x == 'vazio') else x)
    df['C2'] = df['C2'].apply(lambda x: x.strip())
    df['C2'] = df['C2'].apply(lambda x: 'Nenhum' if (x=='vazio') else x)
    df['C3'] = df['C3'].apply(lambda x: 'Desconhecida' if (x=='vazio') else x)
    df['Bairro'] = df['C4'].apply(lambda x: 'Desconhecido' if (x ==  'vazio') else x.split(',')[0].strip())
    df['Cidade'] = df['C4'].apply(lambda x: 'Desconhecido' if (x ==  'vazio') else x.split(',')[1].strip())
    df['C5'] = df['C5'].apply(lambda x: 0 if (x == 'vazio') else x.strip().replace('m²', ''))
    df['C5'] = df['C5'].apply(int)
    df['C6'] = df['C6'].apply(lambda x: 0 if (x == 'vazio') else x.replace('dorms', ''))
    df['C6'] = df['C6'].apply(int)
    df['C7'] = df['C7'].apply(lambda x: '0' if (x=='vazio') else x.split('R$')[1])
    df['C7'] = df['C7'].apply(lambda x: x.strip())
    df['C7'] = df['C7'].apply(lambda x: x.replace('.', ''))
    df['C7'] = df['C7'].apply(int)
    df['C8'] = df['C8'].apply(lambda x: '0' if (x=='vazio') else x.split('R$')[1])
    df['C8'] = df['C8'].apply(lambda x: x.strip())
    df['C8'] = df['C8'].apply(lambda x: x.replace('.', ''))
    df['C8'] = df['C8'].apply(int)

    df.rename(columns={'C0': 'Novo',
                        'C1': 'Tipo',
                        'C2': 'TipoAnuncio',
                        'C3': 'Rua',
                        'C5': 'Metragem',
                        'C6': 'Comodos',
                        'C7': 'Aluguel',
                        'C8': 'Total'}, inplace=True)

    df = df.loc[df['Aluguel'] > 0]
    df = df.loc[df['Total'] > 0]
    df = df.loc[df['Comodos'] > 0]
    df = df.loc[df['Metragem'] > 0]
    df = df.loc[df['Bairro'] != 'Desconhecido']
    df = df.loc[df['Cidade'] != 'Desconhecido']
    df = df.drop('C4', axis=1)

    return df