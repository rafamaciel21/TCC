import ibm_db
import traceback

"""
    Script que armazena as funções de mapeamento de codigo de produto e subproduto.
"""
def mapear_cco(conn, codigo_produto, codigo_produto_derivado):
    """Busca IDPRODUTO e IDSUBPRODUTO na tabela conv.conversao_produto"""
    sql = """
    SELECT idproduto, idsubproduto 
    FROM conv.conversao_produto 
    WHERE codigo_produto = ? 
    AND codigo_produto_derivado = ?
    """
    
    try:
        stmt = ibm_db.prepare(conn, sql)
        ibm_db.bind_param(stmt, 1, str(codigo_produto))
        ibm_db.bind_param(stmt, 2, str(codigo_produto_derivado))
        ibm_db.execute(stmt)
        
        row = ibm_db.fetch_tuple(stmt)
        return {'idproduto': row[0], 'idsubproduto': row[1]} if row else None
    except Exception as e:
        print(f"Erro no mapeamento CCO: {e}")
        return None

def mapear_cba(conn, codigo_barras):
    """Busca IDPRODUTO e IDSUBPRODUTO na tabela PRODUTO_GRADE e PRODUTO_GRADE_CODBARCX"""
    sql = """
    SELECT  
        IDPRODUTO, 
        IDSUBPRODUTO 
    FROM    
        DBA.PRODUTO_GRADE
    WHERE   IDCODBARPROD = ?
    UNION ALL 
    SELECT 
        IDPRODUTO, 
        IDSUBPRODUTO 
    FROM 
        DBA.PRODUTO_GRADE_CODBARCX 
    WHERE   IDCODBARCX = ?
    """
    
    try:
        stmt = ibm_db.prepare(conn, sql)
        ibm_db.bind_param(stmt, 1, str(codigo_barras))
        ibm_db.execute(stmt)
        
        row = ibm_db.fetch_tuple(stmt)
        return {'idproduto': row[0], 'idsubproduto': row[1]} if row else None
    except Exception as e:
        print(f"Erro no mapeamento CBA: {e}")
        return None
    

def mapear_cpd(conn, table_name):
    """
    Mapeamento direto para quando codigo_produto e codigo_produto_derivado
    já são os IDs corretos (CPD = Código Produto derivado)
    Retorna o número de registros atualizados com sucesso
    """
    def execute_sql(conn, sql):
        """Função auxiliar para executar SQL com tratamento de erro"""
        try:
            stmt = ibm_db.exec_immediate(conn, sql)
            if stmt:
                return True
            print(f"Falha ao executar: {sql}")
            return False
        except Exception as e:
            print(f"Erro ao executar SQL: {e}")
            print(f"SQL Problemático: {sql}")
            return False

    try:
        # 1. Atualiza campos temporários
        update_tmp_query = f"""
        UPDATE {table_name}
        SET 
            IDPRODUTO_TMP = TRIM(CODIGO_PRODUTO),
            IDSUBPRODUTO_TMP = TRIM(CODIGO_PRODUTO_DERIVADO),
            OBSERVACAO = 'Mapeado via CPD - IDs diretos'
        WHERE 
            condicao = 'CPD'
            AND (IDPRODUTO IS NULL OR IDSUBPRODUTO IS NULL)
        """
        if not execute_sql(conn, update_tmp_query):
            return 0

        # 2. Verifica e atualiza registros válidos
        verify_query = f"""
        UPDATE {table_name} t
        SET 
            IDPRODUTO = t.IDPRODUTO_TMP,
            IDSUBPRODUTO = t.IDSUBPRODUTO_TMP,
            OBSERVACAO = CONCAT(OBSERVACAO, ' - Validação OK')
        WHERE 
            t.condicao = 'CPD'
            AND EXISTS (
                SELECT 1 
                FROM dba.produto_grade pg
                WHERE pg.IDPRODUTO = t.IDPRODUTO_TMP
                AND pg.IDSUBPRODUTO = t.IDSUBPRODUTO_TMP
            )
        """
        if not execute_sql(conn, verify_query):
            return 0

        # 3. Conta registros atualizados
        count_query = f"""
        SELECT COUNT(*) 
        FROM {table_name} 
        WHERE condicao = 'CPD' 
          AND OBSERVACAO LIKE '%Validação OK%'
          AND IDPRODUTO IS NOT NULL
        """
        stmt = ibm_db.exec_immediate(conn, count_query)
        if stmt:
            count = ibm_db.fetch_tuple(stmt)[0]
        else:
            count = 0

        # 4. Marca registros inválidos
        error_query = f"""
        UPDATE {table_name} t
        SET 
            OBSERVACAO = CONCAT(OBSERVACAO, ' - ERRO: IDs não encontrados'),
            STATUS = 'ERRO'
        WHERE 
            t.condicao = 'CPD'
            AND t.IDPRODUTO IS NULL
            AND NOT EXISTS (
                SELECT 1 
                FROM dba.produto_grade pg
                WHERE pg.IDPRODUTO = t.IDPRODUTO_TMP
                AND pg.IDSUBPRODUTO = t.IDSUBPRODUTO_TMP
            )
        """
        execute_sql(conn, error_query)  # Executa mas não interrompe se falhar

        return count

    except Exception as e:
        print(f"Erro geral no mapeamento CPD: {e}")
        traceback.print_exc()
        return 0