import pymongo
import pandas as pd
import numpy as np
from concat_files import load_csv2df
from pymongo import MongoClient
import json
cluster = MongoClient("mongodb+srv://admin:admin@cluster0-e7ncg.mongodb.net/test?retryWrites=true&w=majority")
db = cluster["productsUp"]
collection = db["todos"]
collections=db.list_collection_names()
def multipleInsertion():
    data = load_csv2df()
    keys = set(data.set_index('Lugar').index)
    def insertinmongoit(item,key):
        db[key].insert_one(json.loads(item.to_json()))
    for x in keys:
        data.set_index('Lugar').loc[x].apply(lambda y: insertinmongoit(y,x),axis=1)

def getData():#Funcion para obtener los datos de la bd
    def makeDataFrame(coll):#esta función recibe como parametro el nombre de la coleccion, ejecuta la funcion find y enlista el cursor para convertirlo en una serie y agregarle el lugar donde se encuentra el producto y devuelve un dataframe en caso de que se hayan encontrado los productos
        tempDoc=pd.Series(list(db[coll].find(toFind)))
        def addPlace(item):
            item["Lugar"]=coll
        tempDoc.apply(addPlace)
        return np.nan if (len(tempDoc)<=0) else pd.DataFrame(tempDoc.tolist())
    toFind={"Producto": {"$in": [ "arroz","cereal","frijoles","jugo"] }}#datos a buscar
    selection=pd.Series(db.list_collection_names()).apply(makeDataFrame)#se crea una serie con el nombre de cada una de las colecciones que posee la base de datos y se les aplica la funcion makeDataFrame
    selection.dropna(inplace=True)
    return (pd.concat(selection.tolist()).drop('_id',axis=1))

def best_price(data):
    selection = (
        data.groupby(['Fecha', 'Lugar', 'Producto'])['Precio'].min()
            .unstack(level=0)
            .swaplevel()
            .sort_index()
            .sort_index(axis=1)
            .fillna(method='ffill', axis=1)
            .iloc[:, -1]
            .unstack()
    )
    def review(item):
        return pd.Series({
            'Suma': item.sum(),
            'p_value': item[item.notna()].size / item.size ,
            'quantity':[item[item.notna()].size, item.size],
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
data=getData()
data=best_price(data)
pd.set_option('display.max_columns', 500)
print(data)

