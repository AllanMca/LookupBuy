import pandas as pd
from LookupBuy.concat_files import load_csv2df

def best_price_by_list(data, lista):
    selection = (
    data.groupby(['Fecha','Lugar', 'Producto'])['Precio'].min()
    .unstack(level=0)
    .swaplevel()
    .sort_index()
    .sort_index(axis=1)
    .fillna(method='ffill', axis=1)
    .iloc[:, -1]
    .unstack()
    .loc[lista]
    )

    def review(item):
        return pd.Series({
            'Suma': item.sum(),
            'p_value': item[item.notna()].size / item.size ,
            'missing': item[item.isna()].index.to_list()
                          })

    def cheapest(item):
        return item[item['Suma'] == item['Suma'].min()].iloc[-1]

    res = (
        selection.apply(review, axis=0)
        .T.reset_index()
        .groupby(['p_value']).apply(cheapest)
    )
    return res

if __name__ == '__main__':
    data = load_csv2df()
    product_list = ['arroz', 'frijoles']
    print(best_price_by_list(data=data, lista=product_list))

    print('-'* 100)
    product_list = ['pan', 'queso', 'tomate']
    print(best_price_by_list(data=data, lista=product_list))

    print('-' * 100)
    product_list = [ 'queso', 'tomate']
    print(best_price_by_list(data=data, lista=product_list))

    print('-' * 100)
    product_list = [ 'cereal', 'jugo']
    print(best_price_by_list(data=data, lista=product_list))
