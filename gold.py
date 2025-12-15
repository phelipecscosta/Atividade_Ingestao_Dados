import duckdb
import os
import pandas as pd

# Diretórios dos dados:
silver_dir = "dataset/silver/**/*.parquet"
gold_dir = "dataset/gold"
gold_output_file = os.path.join(gold_dir, "gastos_agregados_mensal_gold.parquet")

# Garante que o diretório de destino exista, se necessário
os.makedirs(gold_dir, exist_ok=True)

def transform_to_gold(silver_dir: str, gold_dir: str, gold_output_file: str):
    silver_dir = "dataset/silver/**/*.parquet"
gold_dir = "dataset/gold"
gold_output_file = os.path.join(gold_dir, "gastos_agregados_mensal_gold.parquet")

# Garante que o diretório de destino exista
os.makedirs(gold_dir, exist_ok=True)

try:
    conn = duckdb.connect()

    print(f"1. Lendo dados da camada Silver recursivamente em: {silver_dir}")
    
    # 1. Cria uma VIEW temporária para ler todos os arquivos Parquet da camada Silver.
    conn.execute(f"""
        CREATE OR REPLACE VIEW gastos_silver_view AS 
        SELECT 
            codigo_orgao, 
            nome_orgao_superior, 
            codigo_funcao, 
            nome_funcao, 
            valor, 
            ano, 
            mes
        FROM READ_PARQUET('{silver_dir}');
    """)

    # --- 2. Aplicação da Lógica de Negócio (Agregação) ---
    print("2. Executando transformação e agregação (monthly spend by agency/function)...")
    
    aggregation_query = f"""
    SELECT
        CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') AS ano_mes,
        nome_orgao_superior,
        nome_funcao,
        SUM(valor) AS valor_total_gasto,
        COUNT(*) AS num_documentos
    FROM gastos_silver_view
    WHERE valor IS NOT NULL AND valor > 0
    GROUP BY 1, 2, 3
    ORDER BY ano_mes DESC, valor_total_gasto DESC
    """
    
    # Executa a query e armazena o resultado em um DataFrame.
    # O DataFrame é implicitamente registrado no contexto do DuckDB.
    gastos_gold_df = conn.execute(aggregation_query).fetchdf()

    if gastos_gold_df.empty:
        print("A consulta agregada não retornou dados. Verifique a Camada Silver.")
    else:
        # --- 3. Salvamento na Camada Gold ---
        print(f"3. Salvando {len(gastos_gold_df)} registros agregados em {gold_output_file}")
        
        # CORREÇÃO: Usamos o comando SQL COPY TO (equivalente ao EXPORT) para exportar o DataFrame 
        # (agora acessível pelo nome da variável) diretamente para o formato Parquet.
        conn.execute(f"COPY gastos_gold_df TO '{gold_output_file}' (FORMAT PARQUET);")
        
        print(f"Sucesso! Dados Gold prontos para consumo.")

except duckdb.IOException as e:
    print(f"Erro de I/O ao processar arquivos Parquet. Verifique o caminho ou a integridade dos arquivos na camada Silver. Erro: {e}")
except Exception as e:
    print(f"Ocorreu um erro durante o processamento: {e}")
finally:
    if 'conn' in locals() and conn:
        conn.close()


if __name__ == "__main__": 
    transform_to_gold(silver_dir, gold_dir, gold_output_file)