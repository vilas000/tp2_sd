# cluster_sync_app/cluster_sync_peer.py

import socket
import threading
import time
import random
import json
import os
import sys

# --- Configurações ---
CLUSTER_ID = os.getenv("PEER_ID", f"Peer_{random.randint(1000, 9999)}")
HOST = '0.0.0.0'
PORT = int(os.getenv("PEER_PORT"))
ALL_CLUSTER_MEMBERS = [
    ("Peer_1", "sync_peer_1", 12345), ("Peer_2", "sync_peer_2", 12346),
    ("Peer_3", "sync_peer_3", 12347), ("Peer_4", "sync_peer_4", 12348),
    ("Peer_5", "sync_peer_5", 12349)
]

# --- Estado Global ---
lock = threading.Lock()
pending_requests = [] # Fila global de TODAS as requisições no sistema
# CORREÇÃO: Dicionário para gerenciar o estado de cada requisição que ESTE peer é responsável
managed_requests = {} 
deferred_oks = {}
client_sockets_for_reply = {}
active_cluster_members = list(ALL_CLUSTER_MEMBERS)
peer_ready = threading.Event()

def handle_peer_failure(failed_peer_tuple):
    global active_cluster_members
    with lock:
        if failed_peer_tuple in active_cluster_members:
            active_cluster_members.remove(failed_peer_tuple)
            print(f"[{CLUSTER_ID}] DETECTOU FALHA: Peer {failed_peer_tuple[0]} removido. Membros ativos: {len(active_cluster_members)}.")
    check_if_can_enter_critical_section()

def send_message(host, port, message, retries=3, initial_timeout=0.5):
    try:
        with socket.create_connection((host, port), timeout=initial_timeout):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(initial_timeout)
                s.connect((host, port))
                s.sendall(json.dumps(message).encode('utf-8'))
                if message.get("type") == "READY_CHECK":
                    s.recv(1024)
                return True
    except (OSError, socket.timeout):
        return False

def broadcast_to_peers(message):
    with lock: current_active_peers = list(active_cluster_members)
    for peer_tuple in current_active_peers:
        if peer_tuple[0] != CLUSTER_ID:
            if not send_message(peer_tuple[1], peer_tuple[2], message):
                handle_peer_failure(peer_tuple)


def handle_client_request(conn, addr, message):
    global pending_requests, managed_requests, client_sockets_for_reply
    client_id = message["client_id"]
    timestamp = message["timestamp"]
    req_tuple = (timestamp, client_id, CLUSTER_ID)
    req_key = (timestamp, client_id)

    with lock:
        request_exists = any(req[0] == timestamp and req[1] == client_id for req in pending_requests)
        if request_exists:
            print(f"[{CLUSTER_ID}] Requisição duplicada de {client_id}. Respondendo PROCESSING.")
            # CORREÇÃO: Envia uma resposta clara ao cliente antes de fechar a conexão.
            try:
                conn.sendall(b"PROCESSING")
            except Exception as e:
                print(f"[{CLUSTER_ID}] Erro ao enviar PROCESSING: {e}")
            finally:
                conn.close()
            return
        
        print(f"[{CLUSTER_ID}] Iniciando nova requisição para {client_id}.")
        pending_requests.append(req_tuple)
        pending_requests.sort()
        managed_requests[req_key] = {"ok_count": 1, "status": "pending"}
        client_sockets_for_reply[req_key] = conn
    
    broadcast_msg = { "type": "REQUEST_CLUSTER", "requester_peer_id": CLUSTER_ID, "client_id": client_id, "client_timestamp": timestamp }
    broadcast_to_peers(broadcast_msg)
    check_if_can_enter_critical_section()


def handle_cluster_request(message):
    global pending_requests, deferred_oks
    req_peer_id = message["requester_peer_id"]
    client_id = message["client_id"]
    timestamp = message["client_timestamp"]
    new_request_tuple = (timestamp, client_id, req_peer_id)

    with lock:
        if new_request_tuple not in pending_requests:
            pending_requests.append(new_request_tuple)
            pending_requests.sort()
        
        my_managed_requests = [key for key, val in managed_requests.items() if val.get("status") == "pending"]
        should_defer = False
        for my_req_key in my_managed_requests:
            my_req_tuple = (my_req_key[0], my_req_key[1], CLUSTER_ID)
            if my_req_tuple < new_request_tuple:
                should_defer = True
                break
        
        if not should_defer:
            target_tuple = next((p for p in ALL_CLUSTER_MEMBERS if p[0] == req_peer_id), None)
            if target_tuple:
                ok_msg = {"type": "OK", "responding_peer_id": CLUSTER_ID, "target_peer_id": req_peer_id, "request_ts": timestamp, "request_client_id": client_id}
                if not send_message(target_tuple[1], target_tuple[2], ok_msg):
                    handle_peer_failure(target_tuple)
        else:
            deferred_oks[new_request_tuple] = True

def process_ok_response(message):
    global managed_requests
    req_key = (message["request_ts"], message["request_client_id"])
    
    with lock:
        if req_key in managed_requests:
            managed_requests[req_key]["ok_count"] += 1
            num_active = len(active_cluster_members)
            print(f"[{CLUSTER_ID}] Recebeu OK de {message['responding_peer_id']} para {req_key[1]}. Total: {managed_requests[req_key]['ok_count']}/{num_active}.")
    check_if_can_enter_critical_section()

def check_if_can_enter_critical_section():
    global managed_requests
    with lock:
        if not pending_requests: return
        head_of_queue = pending_requests[0]
        req_key = (head_of_queue[0], head_of_queue[1])
        
        if head_of_queue[2] == CLUSTER_ID: # Se este peer é o responsável pela requisição na cabeça da fila
            if req_key in managed_requests and managed_requests[req_key]["status"] == "pending":
                num_active = len(active_cluster_members)
                if managed_requests[req_key]["ok_count"] >= num_active:
                    managed_requests[req_key]["status"] = "in_cs"
                    threading.Thread(target=execute_critical_section, args=(head_of_queue,)).start()

def execute_critical_section(req_tuple):
    global pending_requests, client_sockets_for_reply, managed_requests, deferred_oks
    req_key = (req_tuple[0], req_tuple[1])
    
    print(f"[{CLUSTER_ID}] *** SEÇÃO CRÍTICA INICIADA para {req_key[1]} ***")
    time.sleep(random.uniform(0.2, 1))

    client_socket = client_sockets_for_reply.pop(req_key, None)
    if client_socket:
        try:
            client_socket.sendall(b"COMMITTED")
            print(f"[{CLUSTER_ID}] Enviou COMMITTED para {req_key[1]}.")
        except Exception as e: print(f"[{CLUSTER_ID}] Erro ao enviar COMMITTED: {e}")
        finally: client_socket.close()

    print(f"[{CLUSTER_ID}] *** SEÇÃO CRÍTICA LIBERADA ***")
    
    release_msg = {"type": "RELEASE", "releasing_peer_id": CLUSTER_ID, "client_id": req_key[1], "client_timestamp": req_key[0]}
    broadcast_to_peers(release_msg)
    
    with lock:
        if req_tuple in pending_requests: pending_requests.remove(req_tuple)
        managed_requests.pop(req_key, None)
        
        oks_to_send_now = []
        remaining_deferred = {}
        for deferred_req, _ in deferred_oks.items():
            oks_to_send_now.append(deferred_req)
        deferred_oks = remaining_deferred
    
    for deferred_req_tuple in oks_to_send_now:
        ok_msg = {"type": "OK", "responding_peer_id": CLUSTER_ID, "target_peer_id": deferred_req_tuple[2], "request_ts": deferred_req_tuple[0], "request_client_id": deferred_req_tuple[1]}
        target_info = next((p for p in ALL_CLUSTER_MEMBERS if p[0] == deferred_req_tuple[2]), None)
        if target_info:
            if not send_message(target_info[1], target_info[2], ok_msg):
                handle_peer_failure(target_info)
    
    check_if_can_enter_critical_section()

def handle_release_message(message):
    global pending_requests
    request_to_remove = (message["client_timestamp"], message["client_id"], message["releasing_peer_id"])
    with lock:
        if request_to_remove in pending_requests:
            pending_requests.remove(request_to_remove)
    check_if_can_enter_critical_section()

def handle_connection(conn, addr):
    try:
        data = conn.recv(1024);
        if not data: conn.close(); return
        message = json.loads(data.decode('utf-8'))
        msg_type = message.get("type")

        if msg_type == "REQUEST": peer_ready.wait(); handle_client_request(conn, addr, message)
        else:
            if msg_type == "REQUEST_CLUSTER": handle_cluster_request(message)
            elif msg_type == "OK": process_ok_response(message)
            elif msg_type == "RELEASE": handle_release_message(message)
            elif msg_type == "READY_CHECK": conn.sendall(b"ACK_READY")
            conn.close()
    except Exception: conn.close()

def check_cluster_readiness():
    print(f"[{CLUSTER_ID}] Verificando prontidão do cluster...")
    ready_peers = {CLUSTER_ID}
    while len(ready_peers) < len(ALL_CLUSTER_MEMBERS):
        for peer_id, peer_host, peer_port in ALL_CLUSTER_MEMBERS:
            if peer_id not in ready_peers and send_message(peer_host, peer_port, {"type": "READY_CHECK"}):
                ready_peers.add(peer_id)
                print(f"[{CLUSTER_ID}] Peer {peer_id} está pronto. ({len(ready_peers)}/{len(ALL_CLUSTER_MEMBERS)})")
        time.sleep(1)
    print(f"[{CLUSTER_ID}] TODOS os peers estão online!"); peer_ready.set()

def start_peer_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind((HOST, PORT)); s.listen()
        print(f"[{CLUSTER_ID}] Servidor escutando em {HOST}:{PORT}")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_connection, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    server_thread = threading.Thread(target=start_peer_server, daemon=True)
    readiness_thread = threading.Thread(target=check_cluster_readiness, daemon=True)
    server_thread.start(); readiness_thread.start()
    try:
        readiness_thread.join(); print(f"[{CLUSTER_ID}] Cluster pronto. Peer operacional."); server_thread.join()
    except KeyboardInterrupt: print(f"\n[{CLUSTER_ID}] Encerrando.")