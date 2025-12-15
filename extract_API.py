
# Importando as bibliotecas já instaladas

import requests
import json
import os
import time
import pandas as pd
import glob
import re

#Premissas e restrições
base_url = "https://brasil.io/api/v1/dataset/gastos-diretos/gastos/data/"
output_dir = "dataset/raw" #Endereço da pasta raw
max_pages = 1000  # Limite de 1000 páginas a serem carregadas
api_token = os.environ.get("brasil_token") #Obtém o token em uma variável ambiente (boas práticas de segurança)


#Função para o GET das páginas

def fetch_data_from_api(base_url: str, output_dir: str, max_pages: int, api_token: str = None):
    
    # Faz a chamada paginada para a API, salvando cada captura na camada raw.
    

    # 1. Configuração e Segurança
    os.makedirs(output_dir, exist_ok=True)
    current_url = base_url
    page_count = 1
    headers = {}

    if api_token:
        # Cumprindo requisito da API Brasil.IO: Token no cabeçalho Authorization
        headers['Authorization'] = f'Token {api_token}' 
        print("Usando Token de Autenticação.")
    else:
        # Se o token não for encontrado, encerra devido ao requisito de autenticação (ERRO 401)
        print('ERRO CRÍTICO: Token de API "brasil_token" não definido. Encerrando.')
        return    
       
    print(f"Iniciando ingestão da API: {base_url}. Limite: {max_pages} páginas.")

    #2 loop principal que gerencia a Paginação
    while current_url:
        if page_count > max_pages:
            print(f"Limite de {max_pages} páginas atingido. Encerrando ingestão.")
            break
            
        print(f"-> Ingerindo página {page_count}...")
        
        try:
            # Requisição GET com headers de autenticação
            response = requests.get(current_url, headers=headers)
            response.raise_for_status() # Captura erros HTTP (como 401)

            data = response.json()
            
            # 3. Armazenamento na Camada raw (etapa 8)
            # Salva o dado bruto (raw data) em JSON, essencial para o replay
            filename = os.path.join(output_dir, f"gastos_diretos_page_{page_count}.json")
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            
            print(f"   Arquivo salvo: {filename}")
            
            current_url = data.get("next")
            page_count += 1
            
            # Adiciona delay (boas práticas para Rate Limit e para evitar bloqueio do acesso)
            if current_url and page_count <= max_pages:
                time.sleep(1.5) 


        except requests.exceptions.HTTPError as e:
            # Trata erros específicos de HTTP (como 401)
            print(f"ERRO CRÍTICO (HTTP {response.status_code}) na requisição da página {page_count}: {e}")
            break 
    
        except requests.exceptions.RequestException as e:
            # Trata outros erros de rede/conexão
            print(f"ERRO de Conexão na requisição da página {page_count}: {e}")
            break 

# Ponto de entrada do programa  (boa prática de Software Engineering)
if __name__ == "__main__": #convenção padrão
    fetch_data_from_api(base_url, output_dir, max_pages, api_token)