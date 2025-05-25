
import time

# Marca o tempo de início
start_time = time.time()


from conection.connect_db import conexao_db
from utilities.utilities import create_table_name
import transformations.transformacao as transf
import ibm_db


# Fluxo principal
if __name__ == "__main__":
    conn = conexao_db()
    caminho_arquivo_csv = "C:\\Conversao\\Dados\\estoque_1m.csv"
    table_name = create_table_name("CONV.ESTOQUE_EMPRESA_LOG")
    #table_name = "CONV.TESTE"

    if conn:
        try:
            
            print(f"Tentando criar a tabela: {table_name}")
            transf.create_table(conn, table_name)
            transf.importar_csv_para_tabela_pandas(caminho_arquivo_csv, conn, table_name)
            transf.atualizar_mapeamentos(conn, table_name)
            transf.ajusta_empresa(conn,table_name)
            transf.ajusta_local_estoque(conn,table_name)
            transf.ajusta_qtd(conn, table_name)
            transf.ajusta_custo(conn,table_name)
            transf.get_idplanilha(conn,table_name)
            transf.gera_sequencial(conn,table_name)
            transf.ajustar_datas(conn,table_name)
            transf.insert_tabela_estoque_balanco(conn, table_name)
            transf.gera_relatorio(conn,table_name)

        finally:
            
            # Fecha a conexão
            ibm_db.close(conn)
            print("Conexão encerrada.")

# Marca o tempo de término
end_time = time.time() 


# Calcula o tempo de execução
execution_time = (end_time - start_time)/60
print(f"Tempo total de execução do script: {execution_time:.2f} minutos")