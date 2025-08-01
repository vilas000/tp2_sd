# client_app/client.py

import socket
import time
import random
import datetime
import json
import os
import sys

# --- Configurações ---
CLIENT_ID = os.getenv("CLIENT_ID", f"Cliente_{random.randint(1000, 9999)}")
peers_str = os.getenv("CLUSTER_PEERS_LIST", "localhost:12345")
ALL_PEERS = []
for peer_addr in peers_str.split(','):
    host, port = peer_addr.split(':')
    ALL_PEERS.append((host, int(port)))

# NOVO: Lógica para determinar o índice inicial para o failover circular
try:
    # Extrai a letra do ID do cliente (ex: 'A' de 'Client_A') e a converte para um índice (A=0, B=1, etc.)
    client_char = CLIENT_ID.split('_')[-1]
    my_initial_index = ord(client_char.upper()) - ord('A')
    if my_initial_index < 0 or my_initial_index >= len(ALL_PEERS):
        my_initial_index = random.randint(0, len(ALL_PEERS) - 1)
except:
    my_initial_index = random.randint(0, len(ALL_PEERS) - 1)

def get_peers_in_failover_order(start_index):
    """Retorna a lista de peers na ordem de failover circular."""
    reordered_peers = ALL_PEERS[start_index:] + ALL_PEERS[:start_index]
    return reordered_peers

def iniciar_cliente():
    print(f"[{CLIENT_ID}]: Iniciando... Peers conhecidos: {len(ALL_PEERS)}. Peer inicial: {ALL_PEERS[my_initial_index][0]}")
    time.sleep(random.uniform(1, 5))
    num_acessos = random.randint(10, 50)
    print(f"[{CLIENT_ID}]: Fará {num_acessos} acessos ao Recurso R.")

    acessos_realizados = 0
    current_peer_index = my_initial_index

    for i in range(num_acessos):
        timestamp = datetime.datetime.now().isoformat()
        solicitacao_json = { "type": "REQUEST", "client_id": CLIENT_ID, "timestamp": timestamp }
        
        request_succeeded = False
        # Tenta por todo o anel de peers uma vez
        for _ in range(len(ALL_PEERS)):
            host, port = ALL_PEERS[current_peer_index]
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(5)
                    s.connect((host, port))
                    s.sendall(json.dumps(solicitacao_json).encode('utf-8'))
                    print(f"[{CLIENT_ID}]: Enviou solicitação {i+1} para {host}.")
                    
                    s.settimeout(30)
                    response_data = s.recv(1024).decode('utf-8')
                    
                    if response_data == "COMMITTED":
                        print(f"[{CLIENT_ID}]: Recebeu COMMITTED de {host} para solicitação {i+1}.")
                        acessos_realizados += 1
                        request_succeeded = True
                        break # Sucesso, sai do loop de failover
                    # NOVO: Lida com a resposta PROCESSING
                    elif response_data == "PROCESSING":
                        print(f"[{CLIENT_ID}]: Recebeu PROCESSING de {host}. Requisição está na fila. Continuando...")
                        request_succeeded = True # Considera um sucesso para o ciclo do cliente
                        break
                    else:
                        print(f"[{CLIENT_ID}]: Resposta inesperada de {host}: '{response_data}'")
                        # Move para o próximo peer
                        current_peer_index = (current_peer_index + 1) % len(ALL_PEERS)

            except (ConnectionRefusedError, socket.timeout, OSError) as e:
                print(f"[{CLIENT_ID}]: Falha ao comunicar com {host}: {type(e).__name__}. Tentando próximo peer...")
                # Move para o próximo peer
                current_peer_index = (current_peer_index + 1) % len(ALL_PEERS)
        
        if request_succeeded:
            print(f"[{CLIENT_ID}]: Aguardando de 1 a 5 segundos...")
            time.sleep(random.uniform(1, 5))
        else:
            print(f"[{CLIENT_ID}]: ERRO FATAL: Não foi possível se comunicar com nenhum peer do cluster após uma volta completa.")
            break

    print(f"[{CLIENT_ID}]: Finalizado. Total de {acessos_realizados} acessos COMMITTED.")

if __name__ == "__main__":
    iniciar_cliente()