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
INITIAL_HOST = os.getenv("CLUSTER_SYNC_HOST", '127.0.0.1')
INITIAL_PORT = int(os.getenv("CLUSTER_SYNC_PORT", "12345"))
peers_str = os.getenv("CLUSTER_PEERS_LIST", "")
ALL_PEERS = []
if peers_str:
    for peer_addr in peers_str.split(','):
        host, port = peer_addr.split(':')
        ALL_PEERS.append((host, int(port)))
else:
    ALL_PEERS = [(INITIAL_HOST, INITIAL_PORT)]

def iniciar_cliente():
    print(f"[{CLIENT_ID}]: Iniciando... Peers conhecidos: {len(ALL_PEERS)}")
    time.sleep(random.uniform(1, 5))
    num_acessos = random.randint(10, 50)
    print(f"[{CLIENT_ID}]: Fará {num_acessos} acessos ao Recurso R.")

    acessos_realizados = 0
    # CORREÇÃO: A lógica de failover agora é mais "fixa"
    preferred_peer = (INITIAL_HOST, INITIAL_PORT)

    for i in range(num_acessos):
        timestamp = datetime.datetime.now().isoformat()
        solicitacao_json = { "type": "REQUEST", "client_id": CLIENT_ID, "timestamp": timestamp }
        connected = False
        
        # Tenta o peer preferido primeiro.
        peers_to_try = [preferred_peer] + [p for p in ALL_PEERS if p != preferred_peer]

        for host, port in peers_to_try:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(5)
                    s.connect((host, port))
                    s.sendall(json.dumps(solicitacao_json).encode('utf-8'))
                    print(f"[{CLIENT_ID}]: Enviou solicitação {i+1} para {host}.")
                    
                    s.settimeout(30)
                    data = s.recv(1024)
                    if data.decode('utf-8') == "COMMITTED":
                        print(f"[{CLIENT_ID}]: Recebeu COMMITTED de {host} para solicitação {i+1}.")
                        acessos_realizados += 1
                        # Se a conexão foi bem-sucedida, torna este peer o novo preferido.
                        preferred_peer = (host, port)
                        connected = True
                        break 
                    else:
                        print(f"[{CLIENT_ID}]: Resposta inesperada de {host}: {data.decode('utf-8')}")

            except (ConnectionRefusedError, socket.timeout, OSError) as e:
                print(f"[{CLIENT_ID}]: Falha ao comunicar com {host}: {type(e).__name__}. Tentando próximo peer...")
                # Se o peer preferido falhou, ele será trocado no próximo loop se outro funcionar.
                continue
        
        if connected:
            time.sleep(random.uniform(1, 5))
        else:
            print(f"[{CLIENT_ID}]: ERRO FATAL: Não foi possível conectar a nenhum peer do cluster.")
            break

    print(f"[{CLIENT_ID}]: Finalizado. Total de {acessos_realizados} acessos.")

if __name__ == "__main__":
    iniciar_cliente()