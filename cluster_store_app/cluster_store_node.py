# cluster_store_app/cluster_store_node.py

import socket
import threading
import time
import json
import os
import logging

# --- Configuração do Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')

# --- Configurações do Nó do Cluster Store ---
NODE_ID = os.getenv("NODE_ID", "Store_Unknown")
HOST = '0.0.0.0'
PORT = int(os.getenv("NODE_PORT", "20001"))
PRIMARY_NODE_ID = os.getenv("PRIMARY_NODE_ID", "Store_1")

# Lista de todos os nós do Cluster Store
peers_str = os.getenv("CLUSTER_STORE_PEERS")
ALL_PEERS = []
if peers_str:
    for peer_entry in peers_str.split(','):
        # Agora divide em três partes: ID, host do container e porta
        node_id, container_host, port_str = peer_entry.split(':')
        ALL_PEERS.append({
            "id": node_id,
            "host": container_host,
            "port": int(port_str)
        })

# --- Variáveis de Estado Globais ---
lock = threading.Lock()
role = "BACKUP"  # Pode ser "PRIMARY" ou "BACKUP"
access_counter = 0
access_log = []
active_backups = []
current_primary_id = PRIMARY_NODE_ID

# Evento para sinalizar que o nó está pronto para operar
node_ready = threading.Event()

def send_message(host, port, message, timeout=2):
    """Envia uma mensagem para um nó com timeout e retorna a resposta ou None em caso de falha."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((host, port))
            s.sendall(json.dumps(message).encode('utf-8'))
            
            # Se for uma mensagem que espera resposta, aguarde.
            if message.get("type") in ["WRITE_REQUEST", "GET_STATE"]:
                response_data = s.recv(4096).decode('utf-8')
                return json.loads(response_data)
            return {"status": "SUCCESS"}
    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        logging.error(f"[{NODE_ID}] Falha ao enviar para {host}:{port}. Erro: {e}")
        return None

def replicate_to_backups(data_to_replicate):
    """Função do Primário para replicar dados para todos os backups ativos."""
    global active_backups
    if not active_backups:
        logging.warning(f"[{NODE_ID}-PRIMARY] Nenhum backup ativo para replicar.")
        return True # Sucesso se não houver backups para replicar

    replication_threads = []
    replication_results = []
    
    current_backups = list(active_backups)

    for backup in current_backups:
        msg = {
            "type": "REPLICATE",
            "data": data_to_replicate
        }
        thread = threading.Thread(target=lambda q, b: q.append(send_message(b['host'], b['port'], msg)), args=(replication_results, backup))
        replication_threads.append((thread, backup))
        thread.start()

    for thread, backup in replication_threads:
        thread.join()

    successful_acks = sum(1 for res in replication_results if res and res.get("status") == "ACK_REPLICATE")
    
    if successful_acks < len(current_backups):
        logging.warning(f"[{NODE_ID}-PRIMARY] Falha na replicação. Apenas {successful_acks}/{len(current_backups)} ACKs recebidos.")
        # Lógica para remover backups que falharam
        failed_backups = [b for b, r in zip(current_backups, replication_results) if not (r and r.get("status") == "ACK_REPLICATE")]
        with lock:
            for b in failed_backups:
                if b in active_backups:
                    active_backups.remove(b)
                    logging.info(f"[{NODE_ID}-PRIMARY] Backup {b['id']} removido por falha na replicação.")
    
    # Consideramos sucesso se pelo menos um backup responder, ou se não houver backups.
    # Para consistência forte, o ideal seria esperar todos.
    return successful_acks > 0 or not current_backups


# +++ CÓDIGO NOVO E CORRIGIDO (SEM DEADLOCK) +++
def start_election():
    """Inicia o processo de eleição quando o primário é detectado como falho."""
    global role, current_primary_id

    logging.warning(f"[{NODE_ID}] ELEIÇÃO INICIADA! O primário {current_primary_id} parece ter falhado.")
    
    # --- Lógica de eleição (fora do lock) ---
    potential_primaries = [p for p in ALL_PEERS if p['id'] != current_primary_id]
    if not potential_primaries:
        logging.error(f"[{NODE_ID}] Não há outros nós para se tornarem primário.")
        return
    potential_primaries.sort(key=lambda p: p['id'])
    new_primary = potential_primaries[0]
    logging.info(f"[{NODE_ID}] Novo primário determinado: {new_primary['id']}.")

    i_am_new_primary = (new_primary['id'] == NODE_ID)

    # --- Seção Crítica Mínima (apenas para atualizar variáveis) ---
    with lock:
        current_primary_id = new_primary['id']
        if i_am_new_primary:
            role = "PRIMARY"
        else:
            role = "BACKUP"
    # --- Lock é liberado aqui ---

    # --- Operações de Rede (executadas fora do lock) ---
    if i_am_new_primary:
        logging.info(f"[{NODE_ID}] EU SOU O NOVO PRIMÁRIO!")
        # Tenta obter o estado mais recente de outros backups
        sync_state_from_peers() 
    
    update_active_backups()


def sync_state_from_peers():
    """O novo primário tenta sincronizar seu estado com outros nós."""
    global access_counter, access_log
    
    other_peers = [p for p in ALL_PEERS if p['id'] != NODE_ID]
    latest_counter = -1
    latest_log = None

    for peer in other_peers:
        response = send_message(peer['host'], peer['port'], {"type": "GET_STATE"})
        if response and response.get("status") == "STATE_DATA":
            if response['data']['counter'] > latest_counter:
                latest_counter = response['data']['counter']
                latest_log = response['data']['log']

    if latest_counter > access_counter:
        logging.info(f"[{NODE_ID}-NEW_PRIMARY] Sincronizando estado. Novo contador: {latest_counter}")
        with lock:
            access_counter = latest_counter
            access_log = latest_log


def primary_heartbeat_checker():
    """Função para backups. Verifica se o primário está vivo."""
    while True:
        time.sleep(5)
        if role == "BACKUP" and node_ready.is_set():
            primary_peer = next((p for p in ALL_PEERS if p['id'] == current_primary_id), None)
            if not primary_peer or primary_peer['id'] == NODE_ID:
                continue

            msg = {"type": "PING"}
            if send_message(primary_peer['host'], primary_peer['port'], msg) is None:
                start_election()


def handle_connection(conn):
    """Trata conexões de entrada."""
    global role, access_counter, access_log
    try:
        data = conn.recv(1024).decode('utf-8')
        if not data:
            return
        message = json.loads(data)
        msg_type = message.get("type")

        response = {}

        if msg_type == "WRITE_REQUEST":
            if role == "PRIMARY":
                client_id = message.get("client_id")
                timestamp = message.get("timestamp")
                
                with lock:
                    access_counter += 1
                    log_entry = f"[{timestamp}] Cliente: {client_id}, Contador: {access_counter}"
                    access_log.append(log_entry)
                    logging.info(f"[{NODE_ID}-PRIMARY] Escreveu: {log_entry}")

                    data_to_replicate = {"counter": access_counter, "log": access_log}
                
                if replicate_to_backups(data_to_replicate):
                    response = {"status": "WRITE_SUCCESS"}
                else:
                    response = {"status": "WRITE_FAIL_REPLICATION"}
            else:
                response = {"status": "NOT_PRIMARY", "primary_id": current_primary_id}

        elif msg_type == "REPLICATE":
            if role == "BACKUP":
                with lock:
                    data = message.get("data")
                    access_counter = data.get("counter")
                    access_log = data.get("log")
                    logging.info(f"[{NODE_ID}-BACKUP] Dados replicados. Contador agora é {access_counter}")
                response = {"status": "ACK_REPLICATE"}

        elif msg_type == "PING":
            response = {"status": "PONG"}
        
        elif msg_type == "GET_STATE":
            with lock:
                response = {
                    "status": "STATE_DATA",
                    "data": {"counter": access_counter, "log": access_log}
                }

        else:
            response = {"status": "UNKNOWN_REQUEST"}

        conn.sendall(json.dumps(response).encode('utf-8'))

    except (json.JSONDecodeError, KeyError) as e:
        logging.error(f"[{NODE_ID}] Erro ao processar mensagem: {e}")
    finally:
        conn.close()


def update_active_backups():
    """Atualiza a lista de backups ativos (usado pelo primário)."""
    global active_backups
    if role == "PRIMARY":
        with lock:
            active_backups = [p for p in ALL_PEERS if p['id'] != NODE_ID]
            logging.info(f"[{NODE_ID}-PRIMARY] Backups ativos atualizados: {[b['id'] for b in active_backups]}")

def start_server():
    """Inicia o servidor TCP do nó."""
    global role
    if NODE_ID == PRIMARY_NODE_ID:
        role = "PRIMARY"
        logging.info(f"[{NODE_ID}] Assumindo o papel de PRIMÁRIO.")
    else:
        logging.info(f"[{NODE_ID}] Assumindo o papel de BACKUP.")
    
    update_active_backups()
    node_ready.set()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((HOST, PORT))
    server_socket.listen()
    logging.info(f"[{NODE_ID}] Servidor Store escutando em {HOST}:{PORT}")

    while True:
        conn, _ = server_socket.accept()
        threading.Thread(target=handle_connection, args=(conn,), daemon=True).start()

if __name__ == "__main__":
    threading.Thread(target=primary_heartbeat_checker, daemon=True).start()
    start_server()