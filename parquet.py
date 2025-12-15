import glob
import pandas as pd
import pyarrow.parquet as pq

arquivo = glob.glob("dataset/bronze/gastos_diretos.parquet/ano=2014/mes=7/*.parquet")

table = pq.read_table(arquivo[0])

df = table.to_pandas()

df.to_csv("gastos_diretos_2014.csv", index=False, encoding="utf-8")   

print('Criado na pasta raiz, o arquivo "gastos_diretos_2014.csv" de amostra para análise exploratória')
