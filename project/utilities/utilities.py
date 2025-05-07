import datetime
import time

def create_table_name(base_name):
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    return  f"{base_name}_{timestamp}" ## aqui retorna a tabela com o timestamp
    


