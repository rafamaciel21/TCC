
import time

# Marca o tempo de início
start_time = time.time()


from conection.connect_db import conexao_db
from utilities.utilities import create_table_name
from transformations.transformacao import create_table, importar_csv_para_tabela,atualizar_mapeamentos, ajusta_qtd
import ibm_db





# Fluxo principal
if __name__ == "__main__":
    conn = conexao_db()
    caminho_arquivo_csv = "C:\\Conversao\\Dados\\estoque_10L.csv"
    ##table_name = create_table_name("CONV.ESTOQUE_EMPRESA_LOG")
    table_name = "CONV.TESTE"

    if conn:
        try:
            # Gera o nome da tabela dinamicamente
            
            print(f"Tentando criar a tabela: {table_name}")

            # Chama a função para criar a tabela
            #create_table(conn, table_name)
            #importar_csv_para_tabela(caminho_arquivo_csv, conn, table_name)
            #atualizar_mapeamentos(conn, table_name)
            ajusta_qtd(conn, table_name)

        finally:
            # Fecha a conexão
            ibm_db.close(conn)
            print("Conexão encerrada.")

# Marca o tempo de término
end_time = time.time()

# Calcula o tempo de execução
execution_time = end_time - start_time
print(f"Tempo total de execução do script: {execution_time:.4f} segundos")