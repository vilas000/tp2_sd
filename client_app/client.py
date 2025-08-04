# client_app/client.py

import socket
import time
import random
import datetime
import json
import os

# --- Configurações ---
CLIENT_ID = os.getenv("CLIENT_ID", f"Cliente_{random.randint(1000, 9999)}") # ID do cliente, pode ser definido via variável de ambiente
peers_str = os.getenv("CLUSTER_PEERS_LIST", "localhost:12345") # Lista de peers do cluster, pode ser definida via variável de ambiente
ALL_PEERS = [] # Lista de todos os peers conhecidos
for peer_addr in peers_str.split(','): # Divide a string de peers em endereços individuais
    host, port = peer_addr.split(':') # Divide o endereço em host e porta
    ALL_PEERS.append((host, int(port))) # Adiciona o peer à lista de todos os peers

try:
    # Extrai a letra do ID do cliente (ex: 'A' de 'Client_A') e a converte para um índice (A=0, B=1, etc.)
    client_char = CLIENT_ID.split('_')[-1]
    my_initial_index = ord(client_char.upper()) - ord('A')
    if my_initial_index < 0 or my_initial_index >= len(ALL_PEERS):
        my_initial_index = random.randint(0, len(ALL_PEERS) - 1)
except:
    my_initial_index = random.randint(0, len(ALL_PEERS) - 1) # Se falhar, escolhe um índice aleatório

def get_peers_in_failover_order(start_index): 
    """
    Retorna a lista de peers na ordem de failover circular.
    """
    reordered_peers = ALL_PEERS[start_index:] + ALL_PEERS[:start_index] # Divide a lista de peers a partir do índice inicial e concatena com o início da lista
    return reordered_peers # Retorna a lista de peers na nova ordem

def iniciar_cliente(): 
    """
    Inicia o cliente, realiza acessos ao recurso R e lida com failover.
    """
    print(f"[{CLIENT_ID}]: Iniciando... Peers conhecidos: {len(ALL_PEERS)}. Peer inicial: {ALL_PEERS[my_initial_index][0]}")
    time.sleep(random.uniform(1, 5))
    num_acessos = random.randint(10, 50) # Número de acessos aleatório entre 10 e 50
    print(f"[{CLIENT_ID}]: Fará {num_acessos} acessos ao Recurso R.") 

    acessos_realizados = 0 # Contador de acessos realizados
    current_peer_index = my_initial_index # Índice do peer atual no anel de failover

    for i in range(num_acessos):
        timestamp = datetime.datetime.now().isoformat() # Timestamp da requisição
        solicitacao_json = { "type": "REQUEST", "client_id": CLIENT_ID, "timestamp": timestamp }
        
        request_succeeded = False # Flag para indicar se a requisição foi bem-sucedida
        # Tenta por todo o anel de peers uma vez
        for _ in range(len(ALL_PEERS)): # Loop para tentar cada peer uma vez
            host, port = ALL_PEERS[current_peer_index] # Obtém o host e a porta do peer atual
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: 
                    s.settimeout(5) # Define um timeout de 5 segundos para a conexão
                    s.connect((host, port)) # Conecta ao peer
                    s.sendall(json.dumps(solicitacao_json).encode('utf-8')) # Envia a solicitação ao peer
                    print(f"[{CLIENT_ID}]: Enviou solicitação {i+1} para {host}.")
                    
                    s.settimeout(30) # Define um timeout de 30 segundos para receber a resposta
                    response_data = s.recv(1024).decode('utf-8') # Recebe a resposta do peer e armazena em response_data
                    
                    if response_data == "COMMITTED": 
                        print(f"[{CLIENT_ID}]: Recebeu COMMITTED de {host} para solicitação {i+1}.") 
                        acessos_realizados += 1 # Incrementa o contador de acessos realizados
                        request_succeeded = True # Marca a requisição como bem-sucedida
                        break
                    elif response_data == "PROCESSING":
                        print(f"[{CLIENT_ID}]: Recebeu PROCESSING de {host}. Requisição está na fila. Continuando...")
                        request_succeeded = True # Marca a requisição como bem-sucedida
                        break 
                    else:
                        print(f"[{CLIENT_ID}]: Resposta inesperada de {host}: '{response_data}'")
                        # Continua para o próximo peer se a resposta não for COMMITTED ou PROCESSING
                        current_peer_index = (current_peer_index + 1) % len(ALL_PEERS) 

            except (ConnectionRefusedError, socket.timeout, OSError) as e:
                print(f"[{CLIENT_ID}]: Falha ao comunicar com {host}: {type(e).__name__}. Tentando próximo peer...")
                # Continua para o próximo peer se houver erro de conexão ou timeout
                current_peer_index = (current_peer_index + 1) % len(ALL_PEERS)
        
        if request_succeeded:
            print(f"[{CLIENT_ID}]: Aguardando de 1 a 5 segundos...")
            time.sleep(random.uniform(1, 5)) # Atraso aleatório entre requisições
        else:
            print(f"[{CLIENT_ID}]: ERRO FATAL: Não foi possível se comunicar com nenhum peer do cluster após uma volta completa.")
            break

    print(f"[{CLIENT_ID}]: Finalizado. Total de {acessos_realizados} acessos COMMITTED.")

if __name__ == "__main__":
    iniciar_cliente()