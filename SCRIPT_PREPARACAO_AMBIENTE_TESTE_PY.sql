

/*
 * INSERT DE PRODUTOS GENERICOS 
 */

INSERT INTO DBA.PRODUTO(IDPRODUTO, IDSECAO, IDGRUPO,IDSUBGRUPO, DESCRCOMPRODUTO, VALGRAMAENTRADA , VALGRAMASAIDA,
 DTALTERACAO,
 DTALTERACAOEMBALAGEMSAIDA,
 EMBALAGEMENTRADA,
 EMBALAGEMSAIDA,
 FLAGEXPBALANCA,
 IDCSTIPIENTRADA,
IDCSTIPISAIDA,
IDNATUREZAPISCOFINS,
 MODELO,
PERREDUCAOPISCOFINS,
 TIPOBAIXAMESTRE)
SELECT 
	ROW_NUMBER()OVER() AS IDPRODUTO,
	9999 AS IDSECAO,
	9999 AS IDGRUPO,
	9999 AS IDSUBGRUPO,
	'PRODUTO GENERICO' DESCRCOMPRODUTO,
	1 VALGRAMAENTRADA , 
	1 VALGRAMASAIDA,
	DBA.TODAY() DTALTERACAO,
	DBA.TODAY() DTALTERACAOEMBALAGEMSAIDA,
	'UN' EMBALAGEMENTRADA,
	'UN' EMBALAGEMSAIDA,
	'F' FLAGEXPBALANCA,
	50 IDCSTIPIENTRADA,
	1 IDCSTIPISAIDA,
	NULL IDNATUREZAPISCOFINS,
	'' MODELO,
	0 PERREDUCAOPISCOFINS,
	'I' TIPOBAIXAMESTRE
FROM 
 	DBA.CEP_ECT  
FETCH FIRST 500000 ROWS ONLY 

go
INSERT INTO DBA.PRODUTO_GRADE (
	CODBAR,
	CODBARPRODTRIB,
	CODCEST,
	DESCRRESPRODUTO, 
	DTALTERACAO,
	DTCADASTRO,
	IDCODBARPROD,
	IDCODBARPRODTRIB,
	IDPRODUTO, 
	IDSUBPRODUTO,
	NCM,
	SUBDESCRICAO)
SELECT 
	IDPRODUTO + 222222222 	CODBAR,
	IDPRODUTO + 222222222  CODBARPRODTRIB,
	'0000000'	CODCEST,
	'PRODUTO_GENERICO' AS DESCRRESPRODUTO,
	DBA.TODAY() DTALTERACAO,
	DBA.TODAY()  DTCADASTRO,
	IDPRODUTO + 222222222   IDCODBARPROD,
	IDPRODUTO + 222222222   IDCODBARPRODTRIB,
	P.IDPRODUTO AS IDPRODUTO,
	P.IDPRODUTO AS IDSUBPRODUTO,
	'00000000'NCM,
	'' SUBDESCRICAO
FROM 
	DBA.PRODUTO P
WHERE 
	NOT EXISTS (SELECT 1 FROM DBA.PRODUTO_GRADE B WHERE B.IDPRODUTO = P.IDPRODUTO)
GO
COMMIT

SELECT *
FROM CONV.teste 

DROP TABLE CONV.teste



-------------------------------------
-- LIMPA AMBIENTE 
-------------------------------------

DELETE FROM DBA.ESTOQUE_BALANCO 
GO



CREATE TABLE CONV.TESTE
(
  Step_Timestamp			TIMESTAMP DEFAULT CURRENT TIMESTAMP
, Step_Name 				VARCHAR(100)

, Condicao 					VARCHAR(100)
, Codigo_Produto 			VARCHAR(100)
, Codigo_Produto_Derivado 	VARCHAR(100)
, Codigo_Produto_Externo 	VARCHAR(100)
, Codigo_Barras 			VARCHAR(100)
, Descricao 				VARCHAR(100)
, Data_Balanco 				VARCHAR(100)
, Empresa 					VARCHAR(100)
, Local_Estoque 			VARCHAR(100)
, Lote 						VARCHAR(100)
, Quantidade 				VARCHAR(100)
, Custo_Unitario 			VARCHAR(100)

, IDEMPRESA 				VARCHAR(100)
, IDLOCALESTOQUE 			VARCHAR(100)
, Quantidade_Tmp 			VARCHAR(100)
, Custo_Unitario_Tmp 		VARCHAR(100)
, Data_Balanco_Tmp			VARCHAR(100)
, Quantidade_Novo 			VARCHAR(100)
, Custo_Unitario_Novo 		VARCHAR(100)
, Data_Balanco_Valid 		VARCHAR(100)
, Codigo_Barras_Tmp 		VARCHAR(100)
, IDPRODUTO_Tmp 			VARCHAR(100)
, IDSUBPRODUTO_Tmp			VARCHAR(100)
, IDPRODUTO 				VARCHAR(100)		
, IDSUBPRODUTO 				VARCHAR(100)
, Observacao 				VARCHAR(100)
, IDLOTE 					VARCHAR(100)
, FLAGLOTE 					VARCHAR(100)
, DTBALANCO 				VARCHAR(100)
, IDPLANILHA 				VARCHAR(100)
, NUMSEQUENCIA 				VARCHAR(100)

, Status					VARCHAR(100)
, Error_Description 		VARCHAR(500)
)

COMMIT

:RA

/*
        Regras
        ---------
        - Saldos de estoque negativos não são convertidos;
        - Produtos sem custo especificado no balanço, assumirão o custo do cadastro;
        - Produtos que restarem sem custo, assumem R$ 1,00;
        - Produto marcado para usar lote, que não tem lote informado no balanço, é criado lote "GERAL";
        - Produto que tem lote informado no balanço, mas o produto não está marcado “Utiliza Controle de Lotes”, este é marcado;
        - Produto que tem lote informado no balanço, mas o lote não existe no cadastro, este lote é cadastrado;
        - Saldo de estoque cujo produto esta inativo é desconsiderado no momento do encerramento do balanço.

        Variáveis
        ---------
        - Tabela                                NOME_TABELA_1
        - Código do produto                     Codigo_Produto_ORIGINAL
        - Tabela de produtos                    NOME_TABELA_CONVERSAO_PRODUTO_1
        - Tabela de produtos Cod Prod           Codigo_Produto_ORIGINAL
        - Empresa                               Empresa_ORIGINAL
        - Local de estoque                      Local_Estoque_ORIGINAL
        - Lote                                  Lote_ORIGINAL
        - Quantidade                            Quantidade_ORIGINAL
        - Custo                                 Custo_Unitario_ORIGINAL
        - Data do balanço                       '2020-07-31'
        - Versão                                Nome Cliente Ver 1

*/

-- Ajustar cabeçalho
CALL CONV.SP_AJUSTA_CABECALHO('CONV', 'NOME_TABELA_1')
GO

-- Ajuste pré conversão
CALL CONV.SP_AJUSTE_PRE_CONVERSAO('CONV', 'NOME_TABELA_1')
GO


-----------------------------------------------------------------------------------------------
-- Ajustar corpo da tabela                                                              |
-----------------------------------------------------------------------------------------------
-- Se alguma das colunas utilizadas para conversão não foi informada, será criada:
ALTER TABLE CONV.NOME_TABELA_1 ADD Empresa_ORIGINAL          VARCHAR(100)
GO
ALTER TABLE CONV.NOME_TABELA_1 ADD Local_Estoque_ORIGINAL    VARCHAR(100)
GO
ALTER TABLE CONV.NOME_TABELA_1 ADD Codigo_Produto_ORIGINAL   VARCHAR(100)
GO
ALTER TABLE CONV.NOME_TABELA_1 ADD Lote_ORIGINAL             VARCHAR(100)
GO
ALTER TABLE CONV.NOME_TABELA_1 ADD Quantidade_ORIGINAL       VARCHAR(100)
GO
ALTER TABLE CONV.NOME_TABELA_1 ADD Custo_Unitario_ORIGINAL   VARCHAR(100)
GO

/*
        -- Validar se todas as colunas existem de acordo com layout padrão
        SELECT  Empresa_ORIGINAL, Local_Estoque_ORIGINAL, Codigo_Produto_ORIGINAL, Lote_ORIGINAL, Quantidade_ORIGINAL, Custo_Unitario_ORIGINAL
        FROM    CONV.NOME_TABELA_1

*/


-----------------------------------------------------------------------------------------------
-- NOVOID                                                                               |
-----------------------------------------------------------------------------------------------
ALTER TABLE CONV.NOME_TABELA_1 ADD NOVOIDPRODUTO INTEGER, ADD NOVOIDSUBPRODUTO INTEGER, ADD FLAGCONVER CHAR(1) DEFAULT 'T'
GO
-- SELECT FLAGCONVER, COUNT(*) FROM CONV.NOME_TABELA_CONVERSAO_PRODUTO_1 GROUP BY FLAGCONVER
UPDATE  CONV.NOME_TABELA_1                      A,
        CONV.NOME_TABELA_CONVERSAO_PRODUTO_1    B
SET     A.NOVOIDPRODUTO                         = B.NOVOIDPRODUTO,
        A.NOVOIDSUBPRODUTO                      = B.NOVOIDSUBPRODUTO
WHERE   A.Codigo_Produto_ORIGINAL                        = B.Codigo_Produto_ORIGINAL AND
        B.FLAGCONVER                            IN('T', 'D', 'J')
GO
UPDATE  CONV.NOME_TABELA_1
SET     FLAGCONVER                              = 'F'
WHERE   NOVOIDSUBPRODUTO                        IS NULL
GO


/*
        -- Saldo de estoque não convertida pois o produto não foi convertido Nome Cliente Ver 1                             []
        SELECT * FROM CONV.NOME_TABELA_1 WHERE FLAGCONVER = 'F' AND (NOVOIDSUBPRODUTO IS NULL)

*/


-----------------------------------------------------------------------------------------------
-- Números                                                                              |
-----------------------------------------------------------------------------------------------
/*
        -- Validar formatos não numéricos
        SELECT Quantidade_ORIGINAL, Custo_Unitario_ORIGINAL FROM CONV.NOME_TABELA_1    WHERE ISNUMERIC(Quantidade_ORIGINAL) = 0 OR ISNUMERIC(Custo_Unitario_ORIGINAL) = 0

*/

-- Tratar formatos:
-- 0000,00
UPDATE CONV.NOME_TABELA_1    SET Quantidade_ORIGINAL             = REPLACE(Quantidade_ORIGINAL               , ',', '.')
GO
UPDATE CONV.NOME_TABELA_1    SET Custo_Unitario_ORIGINAL         = REPLACE(Custo_Unitario_ORIGINAL           , ',', '.')
GO
/*
-- 0.000,00
UPDATE CONV.NOME_TABELA_1    SET Quantidade_ORIGINAL             = REPLACE(REPLACE(Quantidade_ORIGINAL       , '.', ''), ',', '.')
GO
UPDATE CONV.NOME_TABELA_1    SET Custo_Unitario_ORIGINAL         = REPLACE(REPLACE(Custo_Unitario_ORIGINAL   , '.', ''), ',', '.')
GO

-- 0.000.00
UPDATE CONV.NOME_TABELA_1    SET Quantidade_ORIGINAL             = ( REPLACE(Quantidade_ORIGINAL              , '.', '') / 100)
GO
UPDATE CONV.NOME_TABELA_1    SET Custo_Unitario_ORIGINAL         = ( REPLACE(Custo_Unitario_ORIGINAL          , '.', '') / 100)
GO
*/

SELECT NOME FROM DBA.CLIENTE_FORNECEDOR cf WHERE 1=1

-- Numeros inválidos
UPDATE CONV.NOME_TABELA_1    SET Quantidade_ORIGINAL             = 0 WHERE Quantidade_ORIGINAL        IS NULL OR TRIM(Quantidade_ORIGINAL      ) = '' OR ISNUMERIC(Quantidade_ORIGINAL          ) = 0
GO
UPDATE CONV.NOME_TABELA_1    SET Custo_Unitario_ORIGINAL         = 0 WHERE Custo_Unitario_ORIGINAL    IS NULL OR TRIM(Custo_Unitario_ORIGINAL  ) = '' OR ISNUMERIC(Custo_Unitario_ORIGINAL      ) = 0
GO


-----------------------------------------------------------------------------------------------
-- Local de estoque e empresa                                                           |
-----------------------------------------------------------------------------------------------

/*
        -- Validar códigos de empresa e locais de estoque criando de/para entre os sistemas
        SELECT Empresa_ORIGINAL, Local_Estoque_ORIGINAL, COUNT(*) FROM CONV.NOME_TABELA_1 GROUP BY Empresa_ORIGINAL, Local_Estoque_ORIGINAL

*/

-- Local de estoque e empresa
ALTER TABLE CONV.NOME_TABELA_1 ADD IDLOCALESTOQUE INTEGER, ADD IDEMPRESA INTEGER
GO

-- Apenas replicar informação
UPDATE  CONV.NOME_TABELA_1
SET     IDLOCALESTOQUE          = Local_Estoque_ORIGINAL,
        IDEMPRESA               = Empresa_ORIGINAL
GO

/*
-- Criar de/para
UPDATE  CONV.NOME_TABELA_1
SET     IDLOCALESTOQUE          = 1,
        IDEMPRESA               = 1
WHERE   Local_Estoque_ORIGINAL           = '?'
GO
UPDATE  CONV.NOME_TABELA_1
SET     IDLOCALESTOQUE          = 2,
        IDEMPRESA               = 2
WHERE   Local_Estoque_ORIGINAL           = '?'
GO
UPDATE  CONV.NOME_TABELA_1
SET     IDLOCALESTOQUE          = 3,
        IDEMPRESA               = 3
WHERE   Local_Estoque_ORIGINAL           = '?'
GO
*/



/*
        -- Validar se ficou tudo certo
        SELECT Empresa_ORIGINAL, Local_Estoque_ORIGINAL FROM CONV.NOME_TABELA_1 WHERE FLAGCONVER IN('T', 'D', 'J') AND Quantidade_ORIGINAL > 0 AND(IDEMPRESA IS NULL OR IDLOCALESTOQUE IS NULL)

*/

-- Inserir empresa e local de estoque no CISS quando não existe
INSERT INTO DBA.EMPRESA(IDEMPRESA)
SELECT  DISTINCT IDEMPRESA
FROM    CONV.NOME_TABELA_1
WHERE   FLAGCONVER              IN('T', 'D', 'J') AND
        Quantidade_ORIGINAL              > 0 AND
        IDEMPRESA               NOT IN(SELECT B.IDEMPRESA FROM DBA.EMPRESA B)
GO
INSERT INTO DBA.ESTOQUE_CADASTRO_LOCAL(IDLOCALESTOQUE)
SELECT  DISTINCT IDLOCALESTOQUE
FROM    CONV.NOME_TABELA_1
WHERE   FLAGCONVER              IN('T', 'D', 'J') AND
        Quantidade_ORIGINAL              > 0 AND
        IDLOCALESTOQUE          NOT IN(SELECT B.IDLOCALESTOQUE FROM DBA.ESTOQUE_CADASTRO_LOCAL B)
GO


-----------------------------------------------------------------------------------------------
-- Lotes                                                                                |
-----------------------------------------------------------------------------------------------
ALTER TABLE CONV.NOME_TABELA_1 ADD IDLOTE VARCHAR(30)
GO
UPDATE  CONV.NOME_TABELA_1 SET IDLOTE      = TRIM(Lote_ORIGINAL)
GO
UPDATE  CONV.NOME_TABELA_1 SET IDLOTE      = NULL          WHERE TRIM(IDLOTE) = ''
GO


-- Marcado para usar lote, mas nao tem lote no balanço | Inserir lote geral
UPDATE  CONV.NOME_TABELA_1 A
SET     A.IDLOTE                = 'GERAL'
WHERE   A.FLAGCONVER            IN('T', 'D', 'J') AND
        A.IDLOTE                IS NULL AND
        A.Quantidade_ORIGINAL            > 0 AND
        A.NOVOIDSUBPRODUTO      IN(SELECT B.IDSUBPRODUTO FROM PRODUTO_GRADE B WHERE B.FLAGLOTE = 'T')
GO


-- Tem lote no balanco mas o lote não existe | Inserir lote no cadastro
INSERT  INTO DBA.PRODUTO_GRADE_LOTE(IDLOTE, IDPRODUTO, IDSUBPRODUTO)
SELECT  DISTINCT A.IDLOTE, A.NOVOIDSUBPRODUTO, A.NOVOIDSUBPRODUTO
FROM
        CONV.NOME_TABELA_1 A
WHERE
        A.FLAGCONVER            IN('T', 'D', 'J') AND
        A.IDLOTE                IS NOT NULL AND
        A.Quantidade_ORIGINAL            > 0 AND
        NOT EXISTS              (       SELECT  1
                                        FROM    DBA.PRODUTO_GRADE_LOTE  B
                                        WHERE   B.IDSUBPRODUTO          = A.NOVOIDSUBPRODUTO AND
                                                B.IDLOTE                = A.IDLOTE
                                )
GO


-- Tem lote no balanco mas nao esta marcado para usar lote | Marcar
UPDATE  DBA.PRODUTO_GRADE       A
SET     A.FLAGLOTE              = 'T'
WHERE   A.FLAGLOTE              = 'F' AND
        A.IDSUBPRODUTO          IN(     SELECT  A.NOVOIDSUBPRODUTO
                                        FROM
                                                CONV.NOME_TABELA_1 A
                                        WHERE
                                                A.FLAGCONVER            IN('T', 'D', 'J') AND
                                                A.IDLOTE                IS NOT NULL AND
                                                A.Quantidade_ORIGINAL            > 0
                                  )
GO


-----------------------------------------------------------------------------------------------
-- Custo                                                                                |
-----------------------------------------------------------------------------------------------
ALTER TABLE CONV.NOME_TABELA_1 ADD FLAGCUSTOALTERADO CHAR(1) DEFAULT 'F'
GO

-- Produtos sem custo especificado no balanço, assumirão o custo do cadastro
UPDATE  CONV.NOME_TABELA_1              A,
        DBA.POLITICA_PRECO_PRODUTO      B
SET     A.Custo_Unitario_ORIGINAL                = B.CUSTOGERENCIAL,
        A.FLAGCUSTOALTERADO             = 'T'
WHERE   A.IDEMPRESA                    = B.IDEMPRESA          AND
A.Codigo_Produto_ORIGINAL                = B.IDPRODUTO           AND
        A.Codigo_Produto_ORIGINAL                = B.IDSUBPRODUTO        AND
        A.Custo_Unitario_ORIGINAL                = 0                     AND
        B.CUSTOGERENCIAL                <> 0
GO
UPDATE  CONV.NOME_TABELA_1              A,
        DBA.POLITICA_PRECO_PRODUTO      B
SET     A.Custo_Unitario_ORIGINAL                = B.CUSTONOTAFISCAL,
        A.FLAGCUSTOALTERADO             = 'T'
WHERE   A.IDEMPRESA                    = B.IDEMPRESA          AND
A.Codigo_Produto_ORIGINAL                = B.IDPRODUTO           AND
        A.Codigo_Produto_ORIGINAL                = B.IDSUBPRODUTO        AND
        A.Custo_Unitario_ORIGINAL                = 0                     AND
        B.CUSTONOTAFISCAL               <> 0
GO
UPDATE  CONV.NOME_TABELA_1              A,
        DBA.POLITICA_PRECO_PRODUTO      B
SET     A.Custo_Unitario_ORIGINAL                = B.CUSTOULTIMACOMPRA,
        A.FLAGCUSTOALTERADO             = 'T'
WHERE   A.IDEMPRESA                    = B.IDEMPRESA          AND
A.Codigo_Produto_ORIGINAL                = B.IDPRODUTO           AND
        A.Codigo_Produto_ORIGINAL                = B.IDSUBPRODUTO        AND
        A.Custo_Unitario_ORIGINAL                = 0                     AND
        B.CUSTOULTIMACOMPRA             <> 0
GO
UPDATE  CONV.NOME_TABELA_1              A,
        DBA.POLITICA_PRECO_PRODUTO      B
SET     A.Custo_Unitario_ORIGINAL                = B.VALCUSTOREPOS,
        A.FLAGCUSTOALTERADO             = 'T'
WHERE   A.IDEMPRESA                    = B.IDEMPRESA          AND
A.Codigo_Produto_ORIGINAL                = B.IDPRODUTO           AND
        A.Codigo_Produto_ORIGINAL                = B.IDSUBPRODUTO        AND
        A.Custo_Unitario_ORIGINAL                = 0                     AND
        B.VALCUSTOREPOS                 <> 0
GO

-- Custo de R$ 1,00 para produtos que estão sem custo
UPDATE  CONV.NOME_TABELA_1
SET     Custo_Unitario_ORIGINAL                  = 1,
        FLAGCUSTOALTERADO               = 'T'
WHERE   FLAGCONVER                      = 'T'                   AND
        (
        Custo_Unitario_ORIGINAL                  IS NULL                 OR
        Custo_Unitario_ORIGINAL                  <= 0
        )                                                       AND
        Quantidade_ORIGINAL                      > 0
GO

/*
        -- Saldo de estoque cujo custo foi alterado Nome Cliente Ver 1                             []
        SELECT * FROM CONV.NOME_TABELA_1 WHERE FLAGCONVER = 'T' AND FLAGCUSTOALTERADO = 'T' AND Quantidade_ORIGINAL > 0

*/


-----------------------------------------------------------------------------------------------
-- Planilha                                                                             |
-----------------------------------------------------------------------------------------------
ALTER TABLE CONV.NOME_TABELA_1  ADD IDPLANILHA INTEGER
GO
BEGIN

    DECLARE     PLANILHA        INTEGER;
    SET         PLANILHA        = -999999;

    -- Enquanto existir um IDPLANILHA null na tabela de conversão, fica em loop:
    WHILE (SELECT TOP(1) 1 FROM CONV.NOME_TABELA_1 WHERE IDPLANILHA IS NULL) = 1 LOOP

        -- Enquanto a planilha definida já existir no balanço ou nesta tabela, adiciona +1
        WHILE PLANILHA IN ( SELECT DISTINCT EB.IDPLANILHA FROM DBA.ESTOQUE_BALANCO                      EB ORDER BY EB.IDPLANILHA ASC ) OR
              PLANILHA IN ( SELECT DISTINCT CO.IDPLANILHA FROM CONV.NOME_TABELA_1                       CO ORDER BY CO.IDPLANILHA ASC )
        LOOP
            SET PLANILHA = PLANILHA + 1

        END LOOP;

        -- Atribui a planilha para uma das variações de empresa e local de estoque
        UPDATE  CONV.NOME_TABELA_1                      A
        SET     A.IDPLANILHA                            = PLANILHA
        WHERE   A.IDPLANILHA                            IS NULL AND
                'EMP ' ||COALESCE(A.IDEMPRESA     , '')||
                ' EST '||COALESCE(A.IDLOCALESTOQUE, '')  =  (   SELECT  MIN(    'EMP ' ||COALESCE(B.IDEMPRESA     , '')||
                                                                                ' EST '||COALESCE(B.IDLOCALESTOQUE, '')
                                                                           )
                                                                FROM    CONV.NOME_TABELA_1            B
                                                                WHERE   B.IDPLANILHA                                    IS NULL
                                                             );
    END LOOP;

END
GO


-----------------------------------------------------------------------------------------------
-- Inserts                                                                              |
-----------------------------------------------------------------------------------------------
DELETE FROM DBA.ESTOQUE_BALANCO
GO
INSERT INTO DBA.ESTOQUE_BALANCO (
        IDLOCALESTOQUE,
        IDEMPRESA,
        IDPLANILHA,
        NUMSEQUENCIA,
        IDPRODUTO,
        IDSUBPRODUTO,
        IDLOTE,
        DTBALANCO,
        QTDCONTADA,
        CUSTOUNITARIO,
        DESCRBALANCO)
SELECT
        PRODUTOS.IDLOCALESTOQUE                 AS IDLOCALESTOQUE,
        PRODUTOS.IDEMPRESA                      AS IDEMPRESA,
        PRODUTOS.IDPLANILHA                     AS IDPLANILHA,
        NUMBER(*)                               AS NUMSEQUENCIA,
        PRODUTOS.NOVOIDPRODUTO                  AS IDPRODUTO,
        PRODUTOS.NOVOIDSUBPRODUTO               AS IDSUBPRODUTO,
        PRODUTOS.IDLOTE                         AS IDLOTE,
        '2020-07-31'                            AS DTBALANCO,
        PRODUTOS.Quantidade_ORIGINAL                     AS QTDCONTADA,
        PRODUTOS.Custo_Unitario_ORIGINAL                 AS CUSTOUNITARIO,
        'BALANÇO ORIGINÁRIO DE CONVERSÃO'       AS DESCRBALANCO
FROM
        CONV.NOME_TABELA_1                      AS PRODUTOS
WHERE
        PRODUTOS.FLAGCONVER                     IN('T', 'D', 'J') AND
        PRODUTOS.Quantidade_ORIGINAL                     > 0
GO
COMMIT