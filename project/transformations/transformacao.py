import ibm_db
import csv
import traceback
import pandas as pd
import ibm_db_dbi
from transformations.mapeamento_codigo import mapear_cba, mapear_cco, mapear_cpd
import os
from datetime import datetime


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
        Step_Timestamp          TIMESTAMP DEFAULT CURRENT TIMESTAMP,
        Step_Name               VARCHAR(100),
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

def importar_csv_para_tabela_pandas(caminho_csv, conn, table_name, batch_size=10000):
    try:
        # Carregar o CSV no Pandas
        df = pd.read_csv(caminho_csv, delimiter=";", encoding="utf-8", dtype=str)
        df.columns = [coluna.strip().upper() for coluna in df.columns]  # Garantir nomes das colunas em maiúsculas
        
        # Substituir valores nulos ou inválidos com string vazia
        df.fillna("", inplace=True)  # Substituir nulos por strings vazias

        # Conectar ao banco usando o adaptador do ibm_db_dbi para Pandas
        db_conn = ibm_db_dbi.Connection(conn)
        
        # Preparar a query de inserção
        colunas = ", ".join(df.columns)
        placeholders = ", ".join(["?"] * len(df.columns))
        query = f"INSERT INTO {table_name} ({colunas}) VALUES ({placeholders})"
        
        # Dividir em lotes para melhor performance
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size].values.tolist()
            try:
                with db_conn.cursor() as cursor:
                    cursor.executemany(query, batch)
                print(f"Lote {i // batch_size + 1} inserido com sucesso.")
            except Exception as e:
                print(f"Erro ao inserir lote {i // batch_size + 1}: {e}")
                traceback.print_exc()
                db_conn.rollback()
                raise
        
        # Commit final
        db_conn.commit()
        print(f"Dados importados com sucesso para {table_name}.")
    
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
        condicao,
        codigo_produto_externo
    FROM {table_name}
    WHERE condicao IN ('CBA', 'CCO','CPD','CPE')
    AND (IDPRODUTO IS NULL OR IDSUBPRODUTO IS NULL)
    """
    
    # Query de atualização usando os códigos como identificadores
    update_query_cco = f"""
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

    update_query_cba = f"""
                            UPDATE {table_name}
                            SET 
                                IDPRODUTO = ?,
                                IDSUBPRODUTO = ?,
                                OBSERVACAO = ?
                            WHERE  
                                CODIGO_BARRAS = ?
                                CONDICAO = ?
                        """
    
    update_query_cpe = f"""
                            UPDATE {table_name}
                            SET 
                                IDPRODUTO = ?,
                                IDSUBPRODUTO = ?,
                                OBSERVACAO = ?
                            WHERE  
                                CODIGO_PRODUTO_EXTERNO = ?
                                CONDICAO = ?
                        """

    total_atualizados_cba = 0
    total_atualizados_cco = 0
    total_atualizados_cpe = 0

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
            codigo_produto_externo = row[4]
            
            mapeamento = None
            obs = ""
            
            if condicao == 'CBA':
                mapeamento = mapear_cba(conn, cod_barras)
                obs = f"Mapeado via CBA - Cód.Barras: {cod_barras}"
            elif condicao == 'CCO':
                mapeamento = mapear_cco(conn, cod_prod, cod_deriv)
                obs = f"Mapeado via CCO - Prod: {cod_prod}, Deriv: {cod_deriv}"
            elif condicao == 'CPE':
                mapeamento = mapear_cco(conn, codigo_produto_externo)
                obs = f"Mapeado via CPE - Prod: {codigo_produto_externo}"
    
    
                
            #aqui executa os mapeamentos para relacionar o dado que está vindo da origem com o dado que existe no destino
            if mapeamento:
                try:
                    if condicao == 'CCO':
                        stmt_update_cco = ibm_db.prepare(conn, update_query_cco)
                        ibm_db.bind_param(stmt_update_cco, 1, mapeamento['idproduto'])
                        ibm_db.bind_param(stmt_update_cco, 2, mapeamento['idsubproduto'])
                        ibm_db.bind_param(stmt_update_cco, 3, obs)
                        ibm_db.bind_param(stmt_update_cco, 4, cod_prod)
                        ibm_db.bind_param(stmt_update_cco, 5, cod_deriv)
                        ibm_db.bind_param(stmt_update_cco, 6, condicao)
                        if ibm_db.execute(stmt_update_cco):
                            total_atualizados_cco += 1
                    elif condicao == 'CBA':
                        stmt_update_cba = ibm_db.prepare(conn, update_query_cba)
                        ibm_db.bind_param(stmt_update_cba, 1, mapeamento['idproduto'])
                        ibm_db.bind_param(stmt_update_cba, 2, mapeamento['idsubproduto'])
                        ibm_db.bind_param(stmt_update_cba, 3, obs)
                        ibm_db.bind_param(stmt_update_cba, 4, cod_barras)
                        ibm_db.bind_param(stmt_update_cba, 5, condicao)
                        if ibm_db.execute(stmt_update_cba):
                            total_atualizados_cba += 1
                    elif condicao == 'CPE':
                        stmt_update_cpe = ibm_db.prepare(conn, update_query_cpe)
                        ibm_db.bind_param(stmt_update_cba, 1, mapeamento['idproduto'])
                        ibm_db.bind_param(stmt_update_cba, 2, mapeamento['idsubproduto'])
                        ibm_db.bind_param(stmt_update_cba, 3, obs)
                        ibm_db.bind_param(stmt_update_cba, 4, codigo_produto_externo)
                        ibm_db.bind_param(stmt_update_cba, 5, condicao)
                        if ibm_db.execute(stmt_update_cba):
                            total_atualizados_cpe += 1

                except Exception as e:
                    print(f"Erro ao atualizar {condicao} - Prod:{cod_prod}, Deriv:{cod_deriv}, Extern:{codigo_produto_externo}, Barras:{cod_barras} : {str(e)}")
            
            row = ibm_db.fetch_tuple(stmt_select)

            
        # 2. Processa CPD (processamento em lote)
        cpd_atualizados = mapear_cpd(conn, table_name)
        if cpd_atualizados is not None:
            print(f"Registros CPD processados: {cpd_atualizados}")
        
        ibm_db.commit(conn)
        print(f"Total de registros mapeados CBA: {total_atualizados_cba}")
        print(f"Total de registros mapeados CBO: {total_atualizados_cco}")
        print(f"Total de registros mapeados CBO: {total_atualizados_cpe}")
        
    except Exception as e:
        ibm_db.rollback(conn)
        print(f"Erro no processamento: {str(e)}")
        ibm_db.rollback(conn)
        raise
    finally:
        ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_ON)



def ajusta_qtd(conn, table_name):
    query_u_qdef = f"""UPDATE {table_name}    SET QUANTIDADE_NOVO        = REPLACE(QUANTIDADE, ',', '.')"""
    query_u_qdef_st = f"UPDATE {table_name}   SET STATUS                = 'Error',     OBSERVACAO  = 'Quantidade Inválida' where QUANTIDADE_NOVO < 0.001"
            
    
    try:
        stmt_u_qdef = ibm_db.prepare(conn,query_u_qdef)
        ibm_db.execute(stmt_u_qdef)

        stmt_u_qdef_st = ibm_db.prepare(conn,query_u_qdef_st)
        ibm_db.execute(stmt_u_qdef_st)

        print("Quantidade ajustada!")
        ibm_db.commit(conn)

    except Exception as e:
        print(f"Erro ao ajustar a quantidade: {e}")
        ibm_db.rollback(conn)
        raise

def ajusta_custo(conn, table_name):
    
    
    query_u_cdef = f"""UPDATE {table_name}  SET CUSTO_UNITARIO_NOVO        = REPLACE(CUSTO_UNITARIO, ',', '.')"""
    query_u_cppp = f"""
                        ALTER TABLE {table_name} ADD OBSERVACAO_CUSTO VARCHAR(100);
                        UPDATE  {table_name} AS A 
                        SET     A.CUSTO_UNITARIO_NOVO = CASE 
                                                            WHEN PPP.CUSTONOTAFISCAL    > 0 THEN PPP.CUSTONOTAFISCAL
                                                            WHEN PPP.CUSTOULTIMACOMPRA  > 0 THEN PPP.CUSTOULTIMACOMPRA
                                                            WHEN PPP.VALCUSTOREPOS      > 0 THEN PPP.VALCUSTOREPOS
                                                            WHEN PPP.CUSTOGERENCIAL     > 0 THEN PPP.CUSTOGERENCIAL
                                                            ELSE '1'
                                                        END,
                                A.OBSERVACAO_CUSTO  = 'Custo alterado para o custo do produto no sistema.'
                        FROM    DBA.POLITICA_PRECO_PRODUTO PPP
                        WHERE   PPP.IDPRODUTO       = A.IDPRODUTO 
                        AND     PPP.IDSUBPRODUTO    = A.IDSUBPRODUTO
                        AND     PPP.IDEMPRESA       = A.IDEMPRESA
                        AND     (   A.CUSTO_UNITARIO_NOVO = 0 OR 
                                    A.CUSTO_UNITARIO_NOVO = '' OR 
                                    A.CUSTO_UNITARIO_NOVO < 0
                                );
                    """    
    
    try:
        stmt_u_cdef = ibm_db.prepare(conn,query_u_cdef)
        ibm_db.execute(stmt_u_cdef)

        stmt_u_cppp = ibm_db.prepare(conn,query_u_cppp)
        ibm_db.execute(stmt_u_cppp)

        print("Custo ajustada!")
        ibm_db.commit(conn)

    except Exception as e:
        print(f"Erro ao ajustar o custo: {e}")
        ibm_db.rollback(conn)
        raise

        

            
def ajusta_empresa(conn, table_name):
    
    
    query_u_emp = f""" UPDATE {table_name} AS A    
                        SET     A.IDEMPRESA        = E.IDEMPRESA
                        FROM    DBA.EMPRESA E
                        WHERE   E.IDEMPRESA         = A.EMPRESA
                    """
    query_u_emp_err = f"UPDATE {table_name}   SET Status = 'Error',     OBSERVACAO  = OBSERVACAO||'|Empresa não existe no sistema!' where IDEMPRESA IS NULL"    
    
    try:
        stmt_u_emp = ibm_db.prepare(conn,query_u_emp)
        ibm_db.execute(stmt_u_emp)

        stmt_u_emp_err = ibm_db.prepare(conn,query_u_emp_err)
        ibm_db.execute(stmt_u_emp_err)

        print("Empresa ajustada!")
        ibm_db.commit(conn)

    except Exception as e:
        print(f"Erro ao ajustar a empresa: {str(e)}")
        ibm_db.rollback(conn)
        raise    


        
def ajusta_local_estoque(conn, table_name):
    
    query_u_lest = f"""  UPDATE {table_name} AS A    
                        SET     A.IDLOCALESTOQUE        = E.IDLOCALESTOQUE
                        FROM    DBA.ESTOQUE_CADASTRO_LOCAL E
                        WHERE   (
                                    COALESCE(E.IDEMPRESABAIXAEST,0)     = A.IDEMPRESA OR 
                                    COALESCE(E.IDEMPRESABAIXAEST,0)     = 0
                                )
                        AND     E.IDLOCALESTOQUE        = A.LOCAL_ESTOQUE
                    """
    query_u_lest_err = f"UPDATE {table_name}   SET Status = 'Error',     OBSERVACAO  = OBSERVACAO||'|Local de estoque não existe no sistema para a empresa!' where IDLOCALESTOQUE IS NULL"    
    
    try:
        stmt_u_lest = ibm_db.prepare(conn,query_u_lest)
        ibm_db.execute(stmt_u_lest)

        stmt_u_lest_err = ibm_db.prepare(conn,query_u_lest_err)
        ibm_db.execute(stmt_u_lest_err)

        print("Local estoque ajustado!")
        ibm_db.commit(conn)

    except Exception as e:
        print(f"Erro ao ajustar a local de estoque: {str(e)}")
        ibm_db.rollback(conn)
        raise    


def get_idplanilha(conn,table_name):
    try:
        get_planilha = """SELECT dba.uf_get_idplanilha ( ) AS IDPLANILHA FROM DBA.DUMMY"""
        stmt_planilha = ibm_db.prepare(conn,get_planilha)
        ibm_db.execute(stmt_planilha)
        row = ibm_db.fetch_tuple(stmt_planilha)

        if not row or row[0] is None:
            raise ValueError("Nenhum valor retornado para IDPLANILHA.")

        idplanilha = row[0]

        query_update = f"""UPDATE {table_name} SET IDPLANILHA = ?"""

        stmt = ibm_db.prepare(conn,query_update)
        ibm_db.bind_param(stmt,1,idplanilha)
        ibm_db.execute(stmt)
    
        ibm_db.commit(conn)
        print(f"Planilha atualizada com o IDPLANILHA = {idplanilha}.")

    except Exception as e:
        print(f"Erro ao tentar capturar o idplanilha: {str(e)}")
        ibm_db.rollback(conn)
        raise

def gera_sequencial(conn, table_name):
    try:
        query = f"""UPDATE {table_name} SET NUMSEQUENCIA = ROW_NUMBER()OVER()"""
        stmt = ibm_db.prepare(conn,query)
        ibm_db.execute(stmt)
        ibm_db.commit

        print("Sequência ajustada com sucesso!")
    except Exception as e:
        print(f"Erro ao gerar sequencia: {str(e)}")
        ibm_db.rollback(conn)
        raise



def ajustar_datas(conn, table_name):
    
    try:
        query = f"UPDATE {table_name} SET DTBALANCO = REPLACE(DATA_BALANCO,'/','-') WHERE LENGTH(DATA_BALANCO) = 10"
        query_error = f"UPDATE {table_name} SET STATUS = 'Error', ERROR_DESCRIPTION = trim(coalesce(ERROR_DESCRIPTION,'')||' Data inválida!') WHERE DTBALANCO is null"

        stmt = ibm_db.prepare(conn,query)
        ibm_db.execute(stmt)

        stmt_e = ibm_db.prepare(conn,query_error)
        ibm_db.execute(stmt_e)

        ibm_db.commit(conn)

        # Contar registros inválidos
        query_s = f"SELECT COUNT(*) FROM {table_name} WHERE DTBALANCO IS NULL"
        stmt_s = ibm_db.exec_immediate(conn, query_s)
        result = ibm_db.fetch_tuple(stmt_s)  # Buscar o resultado
        invalid_count = result[0] if result else 0

        print("Datas ajustadas com sucesso!")
        print(f"Datas inválidas encontradas: {invalid_count}")

    except Exception as e:
        print(f"Erro ao ajustar datas: {e}")
        ibm_db.rollback(conn)
        raise
