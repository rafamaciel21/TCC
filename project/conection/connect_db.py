import os
os.add_dll_directory(r"C:\\Program Files\\IBM\\SQLLIB\\BIN")  # Ajuste o caminho para o local correto
import ibm_db

def conexao_db():
    # Informações de conexão
    dsn = (
        "DATABASE=TESTE;"
        "HOSTNAME=172.22.47.76;"
        "PORT=30123;"
        "PROTOCOL=TCPIP;"
        "UID=dba;"
        "PWD=a9d9p8.E10;"
    )
    try:
        conn = ibm_db.connect(dsn, "", "")
        print("Conexão estabelecida com sucesso.")
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None
    
