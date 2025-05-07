import ibm_db
import csv
import traceback
import pandas as pd
from transformations.mapeamento_codigo import mapear_cba, mapear_cco, mapear_cpd


def tabela_existe(conn, table_name):
    sql = f"SELECT 1 FROM SYSIBM.SYSTABLES WHERE NAME = '{table_name.split('.')[-1]}'"
    stmt = ibm_db.exec_immediate(conn, sql)
    return ibm_db.fetch_row(stmt)


########## Função que cria a tabela para transformações (CORRIGIDA)
def create_table(conn, table_name):
    if tabela_existe(conn, table_name):
        print(f"Tabela {table_name} já existe, removendo...")
        ibm_db.exec_immediate(conn, f"DROP TABLE {table_name}")
        ibm_db.commit(conn)

    create_sql = f"""
    CREATE TABLE {table_name} (
        Step_Timestamp           TIMESTAMP DEFAULT CURRENT TIMESTAMP,
        Step_Name               VARCHAR(100),
        Condicao                VARCHAR(100),
        Codigo_Produto          VARCHAR(100),
        Codigo_Produto_Derivado VARCHAR(100),
        Codigo_Produto_Externo  VARCHAR(100),
        Codigo_Barras           VARCHAR(100),
        Descricao               VARCHAR(100),
        Data_Balanco            VARCHAR(100),
        Empresa                 VARCHAR(100),
        Local_Estoque           VARCHAR(100),
        Lote                    VARCHAR(100),
        Quantidade              VARCHAR(100),
        Custo_Unitario          VARCHAR(100),
        IDEMPRESA               VARCHAR(100),
        IDLOCALESTOQUE          VARCHAR(100),
        IDPRODUTO_TMP           VARCHAR(100),
        IDSUBPRODUTO_TMP        VARCHAR(100),
        Quantidade_Tmp          VARCHAR(100),
        Custo_Unitario_Tmp      VARCHAR(100),
        Data_Balanco_Tmp        VARCHAR(100),
        Quantidade_Novo         VARCHAR(100),
        Custo_Unitario_Novo     VARCHAR(100),
        Data_Balanco_Valid      VARCHAR(100),
        Codigo_Barras_Tmp       VARCHAR(100),
        IDPRODUTO               VARCHAR(100),
        IDSUBPRODUTO            VARCHAR(100),
        Observacao              VARCHAR(500),
        IDLOTE                  VARCHAR(100),
        FLAGLOTE                VARCHAR(100),
        DTBALANCO               VARCHAR(100),
        IDPLANILHA              VARCHAR(100),
        NUMSEQUENCIA            VARCHAR(100),
        Status                  VARCHAR(100),
        Error_Description       VARCHAR(500)
    )
    """
    
    try:
        print(f"Executando SQL:\n{create_sql}")  # Debug: mostrar o SQL que será executado
        stmt = ibm_db.exec_immediate(conn, create_sql)
        if stmt:
            #print(f"Tabela {table_name} criada com sucesso.")
            ibm_db.commit(conn)
            return True
        else:
            print("Falha ao executar comando CREATE TABLE")
            return False
    except Exception as e:
        print(f"Erro detalhado ao criar tabela: {str(e)}")
        print(f"SQLSTATE: {ibm_db.stmt_error()}")
        print(f"SQLCODE: {ibm_db.stmt_errormsg()}")
        ibm_db.rollback(conn)
        raise

########### Função que importa dados (CORRIGIDA)
def importar_csv_para_tabela(caminho_csv, conn, table_name):
    try:
        with open(caminho_csv, mode="r", encoding="utf-8") as arquivo_csv:
            leitor_csv = csv.DictReader(arquivo_csv, delimiter=";")
            colunas = [coluna.strip().upper() for coluna in leitor_csv.fieldnames]
            
            if not colunas:
                raise ValueError("O arquivo CSV não possui cabeçalho ou está vazio.")
            
            # Corrigido: Nomes de colunas e tabela são interpolados diretamente
            # Placeholders são usados apenas para valores
            placeholders = ", ".join(["?"] * len(colunas))
            nomes_colunas = ", ".join(colunas)
            query = f"INSERT INTO {table_name} ({nomes_colunas}) VALUES ({placeholders})"
            
            for linha in leitor_csv:
                linha_maiusculo = {chave.strip().upper(): valor for chave, valor in linha.items()}
                valores = [linha_maiusculo[coluna] for coluna in colunas]
                
                try:
                    stmt = ibm_db.prepare(conn, query)
                    # Agora só precisamos vincular os valores dos dados
                    for i, valor in enumerate(valores, start=1):
                        ibm_db.bind_param(stmt, i, valor)
                    ibm_db.execute(stmt)
                except Exception as e:
                    print(f"Erro ao inserir linha: {linha}")
                    print(f"Query: {query}")
                    print(f"Valores: {valores}")
                    print(f"Erro detalhado: {e}")
                    traceback.print_exc()
                    ibm_db.rollback(conn)
                    return
            
            print(f"Dados importados com sucesso para {table_name}.")
            ibm_db.commit(conn)
    except Exception as e:
        print(f"Erro ao importar CSV: {e}")
        traceback.print_exc()
        ibm_db.rollback(conn)
        raise


def atualizar_mapeamentos(conn, table_name):
    """
    Atualiza IDPRODUTO e IDSUBPRODUTO na tabela de transformação
    baseado na condição (CBA = código de barras, CCO = código produto)
    """
    # Query para buscar registros não mapeados
    select_query = f"""
    SELECT 
        codigo_produto,
        codigo_produto_derivado,
        codigo_barras,
        condicao
    FROM {table_name}
    WHERE condicao IN ('CBA', 'CCO','CPD')
    AND (IDPRODUTO IS NULL OR IDSUBPRODUTO IS NULL)
    """
    
    # Query de atualização usando os códigos como identificadores
    update_query = f"""
    UPDATE {table_name}
    SET 
        IDPRODUTO = ?,
        IDSUBPRODUTO = ?,
        OBSERVACAO = ?
    WHERE 
        codigo_produto = ?
        AND codigo_produto_derivado = ?
        AND condicao = ?
    """
    
    try:
        ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)

        # 1. Processa CBA e CCO (processamento linha a linha)
        stmt_select = ibm_db.exec_immediate(conn, select_query)
        row = ibm_db.fetch_tuple(stmt_select)
        
        total_atualizados = 0
        
        while row:
            cod_prod = row[0]
            cod_deriv = row[1]
            cod_barras = row[2]
            condicao = row[3]
            
            mapeamento = None
            obs = ""
            
            if condicao == 'CBA':
                mapeamento = mapear_cba(conn, cod_barras)
                obs = f"Mapeado via CBA - Cód.Barras: {cod_barras}"
            elif condicao == 'CCO':
                mapeamento = mapear_cco(conn, cod_prod, cod_deriv)
                obs = f"Mapeado via CCO - Prod: {cod_prod}, Deriv: {cod_deriv}"
    
                
            
            if mapeamento:
                try:
                    stmt_update = ibm_db.prepare(conn, update_query)
                    ibm_db.bind_param(stmt_update, 1, mapeamento['idproduto'])
                    ibm_db.bind_param(stmt_update, 2, mapeamento['idsubproduto'])
                    ibm_db.bind_param(stmt_update, 3, obs)
                    ibm_db.bind_param(stmt_update, 4, cod_prod)
                    ibm_db.bind_param(stmt_update, 5, cod_deriv)
                    ibm_db.bind_param(stmt_update, 6, condicao)
                    
                    if ibm_db.execute(stmt_update):
                        total_atualizados += 1
                except Exception as e:
                    print(f"Erro ao atualizar {condicao} - Prod:{cod_prod}, Deriv:{cod_deriv}: {e}")
            
            row = ibm_db.fetch_tuple(stmt_select)

            
        # 2. Processa CPD (processamento em lote)
        cpd_atualizados = mapear_cpd(conn, table_name)
        if cpd_atualizados is not None:
            print(f"Registros CPD processados: {cpd_atualizados}")
        
        ibm_db.commit(conn)
        print(f"Total de registros mapeados (CBA+CCO): {total_atualizados}")
        
    except Exception as e:
        ibm_db.rollback(conn)
        print(f"Erro no processamento: {str(e)}")
        raise
    finally:
        ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_ON)



def ajusta_qtd(conn, table_name):
    query_u_qtmp = f"""UPDATE {table_name}    SET Quantidade_Tmp             = REPLACE(Quantidade               , ',', '.')"""
    query_u_qdef = f"""UPDATE {table_name}    SET QUANTIDADE_NOVO             = Quantidade_Tmp""" # Aqui será criado mais ajuste futuramente
            
    
    try:
        stmt_u_qtmp = ibm_db.prepare(conn,query_u_qtmp)
        ibm_db.execute(stmt_u_qtmp)

        stmt_u_qdef = ibm_db.prepare(conn,query_u_qdef)
        ibm_db.execute(stmt_u_qdef)

        print("Quantidade ajustada!")
        ibm_db.commit(conn)

    except Exception as e:
        print(f"Erro ao ajustar a quantidade: {e}")
        raise

def ajusta_custo(conn, table_name):
    
    
    query_u_ctmp = f"""UPDATE {table_name}    SET Quantidade_Tmp             = REPLACE(Quantidade               , ',', '.')"""
    query_u_cdef = f"""UPDATE {table_name}    SET QUANTIDADE_NOVO             = Quantidade_Tmp""" # Aqui será criado mais ajuste futuramente
            
    
    try:
        stmt_u_ctmp = ibm_db.prepare(conn,query_u_ctmp)
        ibm_db.execute(stmt_u_ctmp)

        stmt_u_cdef = ibm_db.prepare(conn,query_u_cdef)
        ibm_db.execute(stmt_u_cdef)

        print("Quantidade ajustada!")
        ibm_db.commit(conn)

    except Exception as e:
        print(f"Erro ao ajustar a quantidade: {e}")
        raise
            
    

