# Importando as bibliotecas já instaladas

import requests
import json
import os
import time
import pandas as pd
import glob
import re

#Diretórios dos dados
raw_dir = "dataset/raw" #Endereço da pasta raw
bronze_dir = "dataset/bronze" #Endereço da pasta bronze


#Etapa Bronze (Conversão json para parquet)

def transform_to_bronze(raw_dir: str, bronze_dir: str):
    """
    Lê os arquivos JSON da camada RAW, extrai os registros e salva na camada BRONZE
    em formato Parquet particionado por ano e mês.
    """
    print("-" * 50)
    print("Iniciando a transformação RAW -> BRONZE...")
    
    os.makedirs(bronze_dir, exist_ok=True)
    all_data = []

    # 1. Leitura de Todos os Arquivos JSON (Ingestão do RAW)
    # ... (Mantenha o loop de leitura dos arquivos JSON e extração dos 'results')
    json_files = sorted(glob.glob(os.path.join(raw_dir, "*.json")))
    
    if not json_files:
        print(f"AVISO: Nenhuma página JSON encontrada em {raw_dir}.")
        return

    for file_path in json_files:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            results = data.get('results', [])
            if results:
                all_data.extend(results)

    if not all_data:
        print("Nenhum registro de dados encontrado nos arquivos JSON. Encerrando transformação.")
        return

    # 3. Criação do DataFrame Pandas e Tipagem
    df = pd.DataFrame(all_data)
    print(f"Total de {len(df)} registros lidos de {len(json_files)} arquivos JSON.")

    # 4. GARANTIA DE TIPAGEM PARA PARTITIONING (ANO e MES JÁ EXISTEM NO DATASET)
    # Converte colunas de ano/mês para inteiro (necessário para o particionamento)
    # e remove registros onde ano ou mes são nulos (apenas por segurança)
    
    # Primeiro, garanta que os valores são numéricos antes da conversão para int
    df['ano'] = pd.to_numeric(df['ano'], errors='coerce')
    df['mes'] = pd.to_numeric(df['mes'], errors='coerce')
    
    # Remove qualquer linha onde 'ano' ou 'mes' não são válidos (após o coerce)
    df = df.dropna(subset=['ano', 'mes']) 
    
    # Converte para inteiro para uso no particionamento
    df['ano'] = df['ano'].astype(int)
    df['mes'] = df['mes'].astype(int)

    # 5. Escrita na Camada Bronze (Parquet Colunar e Particionado)
    print(f"Escrevendo dados na camada BRONZE em {bronze_dir}...")
    
    # O Parquet é um formato colunar, ideal para otimização de leitura [4].
    df.to_parquet(
        os.path.join(bronze_dir, 'gastos_diretos.parquet'),
        index=False,
        partition_cols=['ano', 'mes'], # Otimização: Particionamento por Ano e Mês [5]
        engine='pyarrow',
        compression='snappy' 
    )

    print(f"Transformação BRONZE concluída. Dados particionados por ano/mes.")
    print("-" * 50)

# Ponto de entrada do programa  (boa prática de Software Engineering)
if __name__ == "__main__": #convenção padrão
    transform_to_bronze(raw_dir, bronze_dir)