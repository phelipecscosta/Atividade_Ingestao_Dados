
import pandas as pd  #Manipular arquivos Parquet
import os      #Cria diretórios (pastas) e toda a hierarquia necessária
import glob    #Buscar arquivos e diretórios usando padrões de nomes
import logging # Para a parte de DataOps (informações sobre o que está acontecendo no programa (erros, avisos, eventos importantes) etc)

# Configuração básica de logging (melhor prática de DataOps/Observabilidade)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
'''
Ativa-se o o logging e define o seu comportamento.
level=logging.INFO Define o nível mínimo de severidade das mensagens que serão exibidas. 
-> INFO: O programa mostrará mensagens de INFO, WARNING, ERROR e CRITICAL
-> FORMAT: Define o formato da saída do log.

          %(asctime)s: insere a data e hora do evento.

          %(levelname)s: insere o nível do log (INFO, ERROR, etc.).

          %(message)s: insere o texto da mensagem que você passou no logging.info(), logging.error(), etc.
'''

# Diretórios dos dados:
bronze_dir = "dataset/bronze" #leitura
silver_dir = "dataset/silver" #escrita

def transform_to_silver(bronze_dir: str, silver_dir: str):
    """
    Lê os dados Parquet da camada BRONZE, aplica limpeza e validação, 
    e salva na camada SILVER.
    """
    logging.info("--- Tarefa: Transformação para Silver iniciada. ---")
    
    # Garantir que a pasta de destino Silver exista
    os.makedirs(silver_dir, exist_ok=True)

    # 1. Lendo os dados da camada Bronze (Particionada)
    # pandas.read_parquet pode ler um diretório inteiro de arquivos particionados
    try:
        # A flag engine='pyarrow' garante que o PyArrow (já instalado) seja usado
        df = pd.read_parquet(bronze_dir, engine='pyarrow')
        
        logging.info(f"Dados do Bronze lidos com sucesso. Total de {len(df)} registros.")

    except Exception as e:
        # Tratamento de exceção (boas práticas de DataOps/Gerenciamento de Incidentes)
        logging.error(f"ERRO CRÍTICO ao ler arquivos do Bronze: {e}")
        return # Encerra a execução em caso de falha crítica

    #2 INÍCIO DAS AÇÕES DE LIMPEZA E TRATAMENTO 
    df_silver = df.copy() #Cria uma cópia do df original para iniciar as transformações

    ##2.1 Assegurando os Tipos corretos

    # Mapeamento para conversões específicas
    date_columns = ['data_pagamento', 'data_pagamento_original']
    float_columns = ['valor']
    missing_values_to_replace = ['', 'NULL', 'NaN', 'N/A']

    # Lista de todas as colunas que devem ser strings (incluindo ano e mes, conforme a tabela)
    # A lista de todas as colunas string é grande, vamos iterar sobre ela de forma otimizada
    string_columns = [
        'codigo_acao', 'codigo_elemento_despesa', 'codigo_favorecido', 'codigo_funcao',
        'codigo_grupo_despesa', 'codigo_orgao', 'codigo_orgao_superior', 'codigo_programa',
        'codigo_subfuncao', 'codigo_unidade_gestora', 'gestao_pagamento', 'linguagem_cidada',
        'nome_acao', 'nome_elemento_despesa', 'nome_favorecido', 'nome_funcao',
        'nome_grupo_despesa', 'nome_orgao', 'nome_orgao_superior', 'nome_programa',
        'nome_subfuncao', 'nome_unidade_gestora', 'numero_documento', 'ano', 'mes'
    ]

    # 2.1.1 Conversão para Float (Numérico)
    for col in float_columns:
        if col in df_silver.columns:
            # pd.to_numeric converte para float. Se um valor for inválido ele será convertido para NaN (Not a Number)
            df_silver[col] = pd.to_numeric(df_silver[col], errors='coerce')

    # 2.1.2 Conversão para Data
    for col in date_columns:
        if col in df_silver.columns:
            # pd.to_datetime converte para datetime. Se um valor for inválido,
            # ele será convertido para NaT (Not a Time)
            # Atenção especial à coluna 'data_pagamento_original' que pode conter strings como 
            # "Detalhamento das informações bloqueado", que se tornarão NaT.
            df_silver[col] = pd.to_datetime(df_silver[col],
                                             format='%d-%m-%Y',
                                               errors='coerce')

    # 2.1.3 Conversão para String
    # Para as colunas que devem ser string, forçamos o tipo.
    # Usamos `.astype(str)` para garantir que todos os valores (incluindo datas ou números) que porventura ainda estejam em outro formato sejam convertidos.
    for col in string_columns:
        if col in df_silver.columns:
            df_silver[col] = df_silver[col].astype(str)

        ### VALOR GASTO ###    
        df_silver['valor'] = pd.to_numeric(df_silver['valor'], errors='coerce') #Converte coluna 'valor' para float e trata erros
        df_silver = df_silver.dropna(subset=['valor']) # Remove registros onde a conversão falhou

    # 2.2 PADRONIZAÇÃO DE NULOS/VAZIOS 

    for col in df_silver.columns:
        # Substituir strings de nulo/vazio por pd.NA (nulo intermediário)
        # Isso padroniza os valores de string ('', 'N/A', etc.) para que fillna() possa pegá-los.
        df_silver[col] = df_silver[col].replace(missing_values_to_replace, pd.NA)
        
        # 'f' representa float. Verificação se o tipo de dado atual é float.
        if df_silver[col].dtype.kind in ['f']: 
            
            # Se for FLOAT, substitui pd.NA (incluindo NaN nativos) por pd.NA, representando NULL
            df_silver[col] = df_silver[col].fillna(pd.NA)
            
        else:
            # Se NÃO for FLOAT (incluindo datetimes e strings), substitui pd.NA (incluindo NaT) pela string 'NA'
            df_silver[col] = df_silver[col].fillna('NA')

    #2.3  Tratamento de Duplicatas (Assumindo que 'id' seria uma chave única)
    logging.info("Aplicando remoção de duplicatas.")
    df_silver = df_silver.drop_duplicates(subset=['codigo_acao'], keep='first')

    # Revertendo ano e mes para string novamente para garantir consistência no particionamento
    df_silver['ano'] = df_silver['ano'].astype(str)
    df_silver['mes'] = df_silver['mes'].astype(str)


   
    # 3 Escrevendo na Camada Silver (Parquet Colunar e Particionado)
    logging.info(f"Escrevendo dados particionados na camada SILVER em {silver_dir}...")

    df_silver.to_parquet(
        silver_dir,
        index=False,
        # Otimização: Particionamento por Ano e Mês
        partition_cols=['ano', 'mes'], 
        engine='pyarrow',
        compression='snappy')

    logging.info("--- Transformação para Silver concluída. ---")


if __name__ == "__main__":   
    transform_to_silver(bronze_dir, silver_dir)