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
# time.sleep(10)

try:
    port_str = os.getenv("PEER_PORT")
    if port_str is None:
        raise ValueError("Variável de ambiente PEER_PORT não definida.")
    PORT = int(port_str)
except (ValueError, TypeError) as e:
    print(f"[{CLUSTER_ID}] ERRO FATAL: Falha na leitura da PEER_PORT no ambiente. Detalhe: {e}", file=sys.stderr)
    sys.exit(1)

# Lista de todos os membros do Cluster Sync
CLUSTER_MEMBERS = [
    ("Peer_1", "sync_peer_1", 12345),
    ("Peer_2", "sync_peer_2", 12346),
    ("Peer_3", "sync_peer_3", 12347),
    ("Peer_4", "sync_peer_4", 12348),
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

# --- Funções Auxiliares ---

def get_current_timestamp():
    return datetime.datetime.now().isoformat()

def send_message(host, port, message, retries=3, initial_timeout=2):
    """
    Envia uma mensagem JSON para um host:port com tentativas de reenvio.
    Retorna a resposta JSON (se READY_CHECK), True para sucesso (sem resposta JSON esperada), False para falha.
    """
    for attempt in range(retries):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(initial_timeout + attempt * 2) # Aumenta timeout a cada tentativa
                s.connect((host, port))
                s.sendall(json.dumps(message).encode('utf-8'))

                if message.get("type") == "READY_CHECK":
                    # Tenta receber a resposta ACK_READY.
                    response_data = s.recv(4096).decode('utf-8')
                    if not response_data:
                        print(f"[{CLUSTER_ID}] Aviso: {host}:{port} fechou conexão sem resposta para READY_CHECK.")
                        return False # Falha na resposta
                    try:
                        return json.loads(response_data) # Retorna a resposta JSON
                    except json.JSONDecodeError:
                        print(f"[{CLUSTER_ID}] Erro JSON na resposta de READY_CHECK de {host}:{port}: '{response_data}'")
                        return False # Resposta não é JSON válida
                return True # Sucesso no envio (nenhuma resposta JSON esperada ou foi processada)
        except (socket.timeout, ConnectionRefusedError) as e:
            print(f"[{CLUSTER_ID}] Erro ao enviar {message.get('type')} para {host}:{port} (Tentativa {attempt+1}/{retries}): {e}")
            time.sleep(0.1 * (attempt + 1)) # Pequeno atraso antes de reintentar
        except Exception as e:
            print(f"[{CLUSTER_ID}] Erro inesperado ao enviar mensagem {message.get('type')} para {host}:{port} (Tentativa {attempt+1}/{retries}): {e}")
            return False 
    print(f"[{CLUSTER_ID}] Falha TOTAL ao enviar {message.get('type')} para {host}:{port} após {retries} tentativas.")
    return False


def broadcast_request_to_peers(client_id, client_timestamp):
    global my_current_request, ok_responses_received

    request_msg = {
        "type": "REQUEST_CLUSTER",
        "requester_peer_id": CLUSTER_ID,
        "client_id": client_id,
        "client_timestamp": client_timestamp
    }

    with lock:
        if my_current_request is not None:
            print(f"[{CLUSTER_ID}] Já estou processando minha requisição para {my_current_request[1]} (ts: {my_current_request[0]}). Ignorando nova de {client_id}.")
            return

        my_current_request = (client_timestamp, client_id, CLUSTER_ID)
        ok_responses_received = 0 
        print(f"[{CLUSTER_ID}] Iniciando nova requisição para {client_id} (ts: {client_timestamp})")

    # IMPORTANTE: Envia para todos os peers ANTES de processar localmente
    for peer_id, peer_host, peer_port in CLUSTER_MEMBERS:
        if peer_id == CLUSTER_ID:
            handle_cluster_request(request_msg) 
        else:
            send_message(peer_host, peer_port, request_msg)
    
    # Agora processa localmente (incluindo adicionar à fila e possivelmente dar OK a si mesmo)
    # handle_cluster_request(request_msg)

def handle_client_request(conn, addr, message): 
    client_id = message["client_id"]
    client_timestamp = message["timestamp"]
    print(f"[{CLUSTER_ID}] Recebeu requisição de cliente: {client_id} (ts: {client_timestamp}) de {addr}")

    with lock:
        client_sockets_for_reply[client_id] = conn 

    broadcast_request_to_peers(client_id, client_timestamp) 

def handle_cluster_request(message):
    global pending_requests, my_current_request, deferred_oks

    req_peer_id = message["requester_peer_id"]
    client_id = message["client_id"]
    client_timestamp = message["client_timestamp"]
    
    with lock:
        new_request_tuple = (client_timestamp, client_id, req_peer_id)
        if new_request_tuple not in pending_requests: 
            pending_requests.append(new_request_tuple)
            pending_requests.sort() 

        print(f"[{CLUSTER_ID}] Fila de requisições atualizada: {pending_requests}")

        my_active_request_info = None
        if my_current_request and my_current_request[2] == CLUSTER_ID:
            my_active_request_info = my_current_request 

        send_ok_immediately = False

        if req_peer_id == CLUSTER_ID:
            print(f"[{CLUSTER_ID}] Processando minha própria requisição {client_id} (ts: {client_timestamp}).")
            return # Sai para não enviar OK para si mesmo.

        if my_active_request_info is None:
            send_ok_immediately = True
        elif client_timestamp < my_active_request_info[0]:
            send_ok_immediately = True
        elif client_timestamp == my_active_request_info[0] and req_peer_id < my_active_request_info[2]:
            send_ok_immediately = True
        
        if send_ok_immediately:
            # if req_peer_id != CLUSTER_ID:
            ok_msg = {
                "type": "OK",
                "responding_peer_id": CLUSTER_ID, # MANTIDO CONSISTENTE
                "target_peer_id": req_peer_id
            }
            if send_message(host_of(req_peer_id), port_of(req_peer_id), ok_msg): # Usar funções auxiliares
                print(f"[{CLUSTER_ID}] Enviou OK imediato para {req_peer_id} (req: {client_id}, ts: {client_timestamp}).")
            else:
                print(f"[{CLUSTER_ID}] FALHA ao enviar OK imediato para {req_peer_id}.")
        else:
            # if req_peer_id != CLUSTER_ID:
            if req_peer_id not in deferred_oks:
                deferred_oks[req_peer_id] = []
            if (client_timestamp, client_id) not in deferred_oks[req_peer_id]:
                deferred_oks[req_peer_id].append((client_timestamp, client_id))
                print(f"[{CLUSTER_ID}] Enfileirou OK para {req_peer_id} (req: {client_id}, ts: {client_timestamp}). Meu ativo: {my_active_request_info[1]}, TS: {my_active_request_info[0]}.")


def process_ok_response(responding_peer_id, target_peer_id):
    global ok_responses_received, my_current_request

    if target_peer_id != CLUSTER_ID:
        print(f"[{CLUSTER_ID}] Recebeu OK de {responding_peer_id} mas target_peer_id é {target_peer_id} (não {CLUSTER_ID}). Ignorando.")
        return

    with lock:
        if my_current_request is not None and my_current_request[2] == CLUSTER_ID:
            ok_responses_received += 1
            print(f"[{CLUSTER_ID}] Recebeu OK de {responding_peer_id}. Total de OKs: {ok_responses_received} de {len(CLUSTER_MEMBERS)-1} necessários para minha requisição {my_current_request[1]}.")


            if ok_responses_received == (len(CLUSTER_MEMBERS) - 1):
                if pending_requests and pending_requests[0] == my_current_request:
                    print(f"[{CLUSTER_ID}] *** TENHO TODOS OS OKs! Entrando na SEÇÃO CRÍTICA com requisição {my_current_request[1]} (ts: {my_current_request[0]}) ***")
                    execute_critical_section()
                else:
                    print(f"[{CLUSTER_ID}] Tenho todos os OKs, mas minha requisição NÃO é a mais prioritária na fila ({my_current_request}). Fila[0]: {pending_requests[0] if pending_requests else 'Vazia'}. Aguardando minha vez.")
        else:
            print(f"[{CLUSTER_ID}] Recebeu OK de {responding_peer_id} para uma requisição que não é a minha atual ({my_current_request}) ou já foi processada.")

def execute_critical_section():
    global my_current_request, ok_responses_received, pending_requests, deferred_oks

    if my_current_request is None:
        print(f"[{CLUSTER_ID}] Erro: Tentativa de entrar na seção crítica sem my_current_request. Isso não deveria acontecer.")
        return

    client_id_to_reply = my_current_request[1]
    
    client_socket = None
    with lock:
        if client_id_to_reply in client_sockets_for_reply:
            client_socket = client_sockets_for_reply.pop(client_id_to_reply)

    if client_socket:
        sleep_time = random.uniform(0.1, 1)
        print(f"[{CLUSTER_ID}] Escrevendo no Recurso R por {sleep_time:.2f}s para {client_id_to_reply}...")
        time.sleep(sleep_time)
        print(f"[{CLUSTER_ID}] Escrita no Recurso R concluída para {client_id_to_reply}.")

        try:
            client_socket.sendall(b"COMMITTED")
            print(f"[{CLUSTER_ID}] Enviou COMMITTED para cliente {client_id_to_reply} via socket original.")
        except Exception as e:
            print(f"[{CLUSTER_ID}] Erro ao enviar COMMITTED para {client_id_to_reply} no socket original: {e}")
        finally:
            client_socket.close() 
            print(f"[{CLUSTER_ID}] Fechou socket original do cliente {client_id_to_reply}.")
    else:
        print(f"[{CLUSTER_ID}] Aviso: Não encontrou socket original do cliente {client_id_to_reply} para responder. Ele pode ter desconectado ou o socket foi fechado de alguma forma.")
        
    with lock:
        current_req_tuple = my_current_request 
        pending_requests = [req for req in pending_requests
                            if not (req[0] == current_req_tuple[0] and 
                                    req[1] == current_req_tuple[1] and 
                                    req[2] == current_req_tuple[2])]

        my_current_request = None
        ok_responses_received = 0
        print(f"[{CLUSTER_ID}] Seção crítica liberada. Estado resetado. Fila atual: {pending_requests}")

        peers_with_deferred = list(deferred_oks.keys()) 
        for peer_id_to_ok in peers_with_deferred:
            target_host, target_port = next((p[1], p[2]) for p in CLUSTER_MEMBERS if p[0] == peer_id_to_ok)
            
            ok_msg = {
                "type": "OK",
                "responding_peer_id": CLUSTER_ID, # MANTIDO CONSISTENTE!
                "target_peer_id": peer_id_to_ok
            }
            if send_message(target_host, target_port, ok_msg):
                print(f"[{CLUSTER_ID}] Enviou OK atrasado para {peer_id_to_ok} (origem: {deferred_oks[peer_id_to_ok]}).")
            else:
                print(f"[{CLUSTER_ID}] FALHA ao enviar OK atrasado para {peer_id_to_ok}.")
        
        deferred_oks.clear()


def handle_connection(conn, addr):
    try:
        conn.settimeout(5) 
        data = conn.recv(4096).decode('utf-8')
        
        # >>>>> Adição: Verifica se data não está vazio antes de tentar JSON <<<<<
        if not data:
            print(f"[{CLUSTER_ID}] Conexão de {addr} fechada ou enviou dados vazios.")
            conn.close()
            return

        message = json.loads(data)

        if message.get("type") == "REQUEST": 
            peer_ready.wait(timeout=30) 
            if not peer_ready.is_set():
                print(f"[{CLUSTER_ID}] Aviso: Não pronto para processar requisição de cliente de {addr} (cluster não pronto em 30s). Ignorando e fechando conexão.")
                conn.close() 
                return
            handle_client_request(conn, addr, message) 
        
        elif message.get("type") == "READY_CHECK":
            print(f"[{CLUSTER_ID}] Recebeu READY_CHECK de {message.get('requester_peer_id')}.")
            ack_msg = {"type": "ACK_READY", "responding_peer_id": CLUSTER_ID}
            conn.sendall(json.dumps(ack_msg).encode('utf-8'))
            conn.close()
        
        elif message.get("type") == "REQUEST_CLUSTER":
            handle_cluster_request(message)
            conn.close() 
        elif message.get("type") == "OK":
            process_ok_response(message.get("responding_peer_id"), message["target_peer_id"]) 
            conn.close() 
        else:
            print(f"[{CLUSTER_ID}] Mensagem desconhecida recebida de {addr}: {message}")
            conn.close() 

    except json.JSONDecodeError:
        print(f"[{CLUSTER_ID}] Erro de JSON: Recebeu mensagem não JSON de {addr}: '{data}'. Ignorando.")
        conn.close() 
    except socket.timeout:
        print(f"[{CLUSTER_ID}] Timeout na leitura inicial da conexão de {addr}. Nenhuma mensagem recebida ou incompleta. Conexão fechada.")
        conn.close()
    except Exception as e:
        print(f"[{CLUSTER_ID}] Erro inesperado ao lidar com conexão de {addr}: {e}")
        # >>>>> Importante: Imprimir o traceback para erros inesperados <<<<<
        traceback.print_exc(file=sys.stderr)
        conn.close()


def handle_cluster_message(conn, message):
    try:
        if message.get("type") == "REQUEST_CLUSTER":
            handle_cluster_request(message)
        elif message.get("type") == "OK":
            process_ok_response(message["responding_peer_id"], message["target_peer_id"])
        elif message.get("type") == "READY_CHECK":
            print(f"[{CLUSTER_ID}] Recebeu READY_CHECK de {message.get('requester_peer_id')}")
            ack_msg = {
                "type": "ACK_READY", 
                "responding_peer_id": CLUSTER_ID,
                "timestamp": datetime.datetime.now().isoformat()
            }
            conn.sendall(json.dumps(ack_msg).encode('utf-8'))
    except Exception as e:
        print(f"[{CLUSTER_ID}] Erro ao processar mensagem do cluster: {e}")
    finally:
        try:
            conn.close()
        except:
            pass

def host_of(peer_id):
    return next((p[1] for p in CLUSTER_MEMBERS if p[0] == peer_id), None)

def port_of(peer_id):
    return next((p[2] for p in CLUSTER_MEMBERS if p[0] == peer_id), None)

def check_cluster_readiness():
    print(f"[{CLUSTER_ID}] Verificando prontidão do cluster...")
    while True:
        all_peers_ready = True
        peers_not_ready = []
        for peer_id, peer_host, peer_port in CLUSTER_MEMBERS:
            if peer_id == CLUSTER_ID:
                continue 
            
            check_msg = {"type": "READY_CHECK", "requester_peer_id": CLUSTER_ID}
            response = send_message(peer_host, peer_port, check_msg) 
            
            if response and response.get("type") == "ACK_READY" and response.get("responding_peer_id") == peer_id:
                pass
            else:
                peers_not_ready.append(peer_id)
                all_peers_ready = False
        
        if all_peers_ready:
            print(f"[{CLUSTER_ID}] TODOS os peers estão online e respondendo! Sinalizando prontidão.")
            peer_ready.set() 
            break
        else:
            print(f"[{CLUSTER_ID}] Peers NÃO prontos ou não respondendo ao READY_CHECK: {peers_not_ready}. Reintentando em 2 segundos...")
        
        time.sleep(2)
        

def start_peer_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
        s.bind((HOST, PORT)) 
        s.listen() 
        print(f"[{CLUSTER_ID}] Servidor escutando em {HOST}:{PORT}")

        while True:
            try:
                conn, addr = s.accept() 
                thread = threading.Thread(target=handle_connection, args=(conn, addr))
                thread.daemon = True 
                thread.start()
            except Exception as e:
                print(f"[{CLUSTER_ID}] Erro no loop de aceitação de conexão do servidor: {e}")


if __name__ == "__main__":
    try: # Este try/except é para capturar erros iniciais no bloco principal
        print(f"[{CLUSTER_ID}] Meu ID: {CLUSTER_ID}, Meu Endereço: {HOST}:{PORT}")
        print(f"[{CLUSTER_ID}] Membros do Cluster Conhecidos: {CLUSTER_MEMBERS}")

        server_thread = threading.Thread(target=start_peer_server)
        server_thread.daemon = True 
        server_thread.start()

        readiness_thread = threading.Thread(target=check_cluster_readiness)
        readiness_thread.daemon = True
        readiness_thread.start()

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"[{CLUSTER_ID}] Peer encerrado manualmente.")
    except Exception as main_block_error:
        print(f"[{CLUSTER_ID}] ERRO INESPERADO NO BLOCO PRINCIPAL: {main_block_error}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)