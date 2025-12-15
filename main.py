import os

def executar_script(nome_arquivo):
    caminho = os.path.join(os.path.dirname(__file__), nome_arquivo)
    os.system(f'python "{caminho}"')

def main():
    print("\n=== MENU PRINCIPAL ===")
    print("1 - Extrair dados da API Brasil")
    print("2 - Transformar camada Bronze")
    print("3 - Gerar amostra para análise exploratória")
    print("4 - Transformar camada Silver")
    print("5 - Transformar camada Gold")
    print("0 - Sair")

    opcao = input("\nEscolha uma opção: ")

    if opcao == "1":
        executar_script("extract_API.py")python main.py 
    elif opcao == "2":
        executar_script("bronze.py")
    elif opcao == "3":
        executar_script("parquet.py")
    elif opcao == "4":
        executar_script("silver.py")
    elif opcao == "5":
        executar_script("gold.py")
    elif opcao == "0":
        print("Saindo...")
    else:
        print("Opção inválida!")

if __name__ == "__main__":
    main()
