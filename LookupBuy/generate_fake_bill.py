import numpy as np
import pandas as pd
import datetime

products = [
    'leche',
    'pan',
    'queso',
    'jugo',
    'arroz',
    'frijoles',
    'cereal',
    'tomate',
    'pepino',
    'azucar'
]

places = ['pali santa ana',
          'automercado heredia',
          'masxmenos belen',
          'perimercados escazu',
          'pali heredia',
          'super la confianza']

N = np.random.randint(5,10)

def get_current_time():
    return datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f')

def generate_products():
    return np.random.choice(a=products, size=N)

def generate_places():
    return np.full(N, np.random.choice(a=places, size=1))

def generate_date():
    return np.random.choice(pd.date_range('20000101', periods=5000))

def generate_prices():
    return np.around(np.random.uniform(low=1000, high=10000, size=N), decimals=2, out=None)

def create_dataframe():
    return pd.DataFrame({
        'Fecha' : generate_date(),
        'Producto' : generate_products(),
        'Lugar' : generate_places(),
        'Precio' : generate_prices()
    })

if __name__ == '__main__':
    df = create_dataframe()
    df.to_csv(f'factura-{get_current_time()}.csv')