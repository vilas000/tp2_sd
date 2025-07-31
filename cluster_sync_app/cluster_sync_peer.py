import socket
import threading
import time
import random
import datetime
import json
import os
import sys
import traceback

# --- Configurações Iniciais ---
CLUSTER_ID = os.getenv("PEER_ID", f"Peer_{random.randint(1000, 9999)}")
HOST = '0.0.0.0'

try:
    port_str = os.getenv("PEER_PORT")
    if port_str is None:
        raise ValueError("Variável de ambiente PEER_PORT não definida.")
    PORT = int(port_str)
except (ValueError, TypeError) as e:
    print(f"[{CLUSTER_ID}] ERRO FATAL: Falha na leitura da PEER_PORT. Detalhe: {e}", file=sys.stderr)
    sys.exit(1)

CLUSTER_MEMBERS = [
    ("Peer_1", "sync_peer_1", 12345), ("Peer_2", "sync_peer_2", 12346),
    ("Peer_3", "sync_peer_3", 12347), ("Peer_4", "sync_peer_4", 12348),
    ("Peer_5", "sync_peer_5", 12349)
]

# --- Estado Global do Peer ---
lock = threading.Lock()
pending_requests = []
my_current_request = None
ok_responses_received = 0
client_sockets_for_reply = {}
deferred_oks = {}
peer_ready = threading.Event()

def send_message(host, port, message, retries=5, initial_timeout=1.0):
    for attempt in range(retries):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(initial_timeout + attempt)
                s.connect((host, port))
                s.sendall(json.dumps(message).encode('utf-8'))
                if message.get("type") == "READY_CHECK":
                    response_data = s.recv(1024).decode('utf-8')
                    return json.loads(response_data) if response_data else None
                return True
        except (OSError, socket.timeout) as e:
            if attempt >= retries - 1: return False
            time.sleep(0.5 * (attempt + 1))
        except Exception: return False
    return False

def broadcast_to_peers(message):
    for peer_id, peer_host, peer_port in CLUSTER_MEMBERS:
        if peer_id != CLUSTER_ID:
            send_message(peer_host, peer_port, message)

def broadcast_request_to_peers(client_id, client_timestamp):
    global my_current_request, ok_responses_received
    with lock:
        my_current_request = (client_timestamp, client_id, CLUSTER_ID)
        ok_responses_received = 0
        pending_requests.append(my_current_request)
        pending_requests.sort()
        print(f"[{CLUSTER_ID}] Iniciando nova requisição para {client_id}. Fila: {pending_requests}")

    request_msg = { "type": "REQUEST_CLUSTER", "requester_peer_id": CLUSTER_ID,
                    "client_id": client_id, "client_timestamp": client_timestamp }
    broadcast_to_peers(request_msg)
    process_ok_response(CLUSTER_ID, CLUSTER_ID)

def handle_client_request(conn, addr, message):
    with lock:
        client_sockets_for_reply[message["client_id"]] = conn
    broadcast_request_to_peers(message["client_id"], message["timestamp"])

def handle_cluster_request(message):
    global deferred_oks
    req_peer_id = message["requester_peer_id"]
    new_request_tuple = (message["client_timestamp"], message["client_id"], req_peer_id)
    with lock:
        if new_request_tuple not in pending_requests:
            pending_requests.append(new_request_tuple)
            pending_requests.sort()
        my_req_info = my_current_request
        send_ok = (my_req_info is None) or (new_request_tuple < my_req_info)
        if send_ok:
            target_host, target_port = next(((p[1], p[2]) for p in CLUSTER_MEMBERS if p[0] == req_peer_id), (None, None))
            if target_host:
                ok_msg = {"type": "OK", "responding_peer_id": CLUSTER_ID, "target_peer_id": req_peer_id}
                send_message(target_host, target_port, ok_msg)
        else:
            deferred_oks[req_peer_id] = True

def process_ok_response(responding_peer_id, target_peer_id):
    global ok_responses_received
    if target_peer_id != CLUSTER_ID: return
    with lock:
        if my_current_request is None: return
        ok_responses_received += 1
        print(f"[{CLUSTER_ID}] Recebeu OK de {responding_peer_id}. Total: {ok_responses_received}/{len(CLUSTER_MEMBERS)}.")
        if ok_responses_received == len(CLUSTER_MEMBERS) and pending_requests[0] == my_current_request:
            execute_critical_section()

# ============================================================================
# ADIÇÃO: Handler para a nova mensagem RELEASE
# ============================================================================
def handle_release_message(message):
    global pending_requests
    with lock:
        request_to_remove = (message["client_timestamp"], message["client_id"], message["releasing_peer_id"])
        if request_to_remove in pending_requests:
            pending_requests.remove(request_to_remove)
            print(f"[{CLUSTER_ID}] Recebeu RELEASE de {message['releasing_peer_id']}. Fila atual: {pending_requests}")
        # Após remover um item, verifica se agora é minha vez
        if my_current_request and ok_responses_received == len(CLUSTER_MEMBERS) and pending_requests and pending_requests[0] == my_current_request:
            execute_critical_section()


def execute_critical_section():
    global pending_requests, my_current_request, ok_responses_received, deferred_oks


    print(f"[{CLUSTER_ID}] *** SEÇÃO CRÍTICA INICIADA para {my_current_request[1]} ***")
    
    # Prepara dados para I/O antes de modificar o estado
    req_tuple = my_current_request
    client_socket = client_sockets_for_reply.pop(req_tuple[1], None)
    
    # Modifica o estado atômicamente
    pending_requests.pop(0)
    my_current_request = None
    ok_responses_received = 0
    peers_to_send_ok = list(deferred_oks.keys())
    deferred_oks.clear()
    
    # O lock será liberado aqui. Operações lentas ocorrem abaixo.
    
    sleep_time = random.uniform(0.2, 1)
    print(f"[{CLUSTER_ID}] ...Trabalhando por {sleep_time:.2f}s...")
    time.sleep(sleep_time)
    
    if client_socket:
        try:
            client_socket.sendall(b"COMMITTED")
            print(f"[{CLUSTER_ID}] Enviou COMMITTED para {req_tuple[1]}.")
        except Exception as e: print(f"[{CLUSTER_ID}] Erro ao enviar COMMITTED: {e}")
        finally: client_socket.close()

    print(f"[{CLUSTER_ID}] *** SEÇÃO CRÍTICA LIBERADA ***")

    # ============================================================================
    # ALTERAÇÃO: Transmitir a mensagem RELEASE para todos os outros peers
    # ============================================================================
    release_msg = { "type": "RELEASE", "releasing_peer_id": CLUSTER_ID,
                    "client_id": req_tuple[1], "client_timestamp": req_tuple[0] }
    broadcast_to_peers(release_msg)

    # Enviar OKs que estavam adiados
    for peer_id in peers_to_send_ok:
        target_host, target_port = next(((p[1], p[2]) for p in CLUSTER_MEMBERS if p[0] == peer_id), (None, None))
        if target_host:
            ok_msg = {"type": "OK", "responding_peer_id": CLUSTER_ID, "target_peer_id": peer_id}
            send_message(target_host, target_port, ok_msg)


def handle_connection(conn, addr):
    try:
        data = conn.recv(1024).decode('utf-8')
        if not data: conn.close(); return
        message = json.loads(data)
        msg_type = message.get("type")

        if msg_type == "REQUEST":
            peer_ready.wait()
            handle_client_request(conn, addr, message)
        else:
            if msg_type == "REQUEST_CLUSTER": handle_cluster_request(message)
            elif msg_type == "OK": process_ok_response(message.get("responding_peer_id"), message["target_peer_id"])
            elif msg_type == "RELEASE": handle_release_message(message) # <-- ADICIONADO
            elif msg_type == "READY_CHECK":
                conn.sendall(json.dumps({"type": "ACK_READY"}).encode('utf-8'))
            conn.close()
    except Exception: conn.close()


def check_cluster_readiness():
    print(f"[{CLUSTER_ID}] Verificando prontidão do cluster...")
    ready_peers = {CLUSTER_ID}
    while len(ready_peers) < len(CLUSTER_MEMBERS):
        for peer_id, peer_host, peer_port in CLUSTER_MEMBERS:
            if peer_id in ready_peers: continue
            if send_message(peer_host, peer_port, {"type": "READY_CHECK"}):
                ready_peers.add(peer_id)
                print(f"[{CLUSTER_ID}] Peer {peer_id} está pronto. ({len(ready_peers)}/{len(CLUSTER_MEMBERS)})")
        if len(ready_peers) < len(CLUSTER_MEMBERS): time.sleep(1)
    print(f"[{CLUSTER_ID}] TODOS os peers estão online!")
    peer_ready.set()


def start_peer_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen()
        print(f"[{CLUSTER_ID}] Servidor escutando em {HOST}:{PORT}")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_connection, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    print(f"[{CLUSTER_ID}] Peer iniciando... Meu ID: {CLUSTER_ID}, Endereço: {HOST}:{PORT}")
    server_thread = threading.Thread(target=start_peer_server, daemon=True)
    readiness_thread = threading.Thread(target=check_cluster_readiness, daemon=True)
    server_thread.start()
    readiness_thread.start()
    try:
        readiness_thread.join()
        print(f"[{CLUSTER_ID}] Cluster pronto. Peer operacional.")
        server_thread.join()
    except KeyboardInterrupt: print(f"\n[{CLUSTER_ID}] Encerrando.")