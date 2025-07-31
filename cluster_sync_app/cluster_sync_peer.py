import socket
import threading
import time
import random
#import datetime
import json
import os
import sys
#import traceback

# configuracoes iniciais do peer, serao lidos das variaveis de ambiente do docker compose
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

# lista de peers do cluster para que cada peer saiba quem sao os outros
CLUSTER_MEMBERS = [
    ("Peer_1", "sync_peer_1", 12345), ("Peer_2", "sync_peer_2", 12346),
    ("Peer_3", "sync_peer_3", 12347), ("Peer_4", "sync_peer_4", 12348),
    ("Peer_5", "sync_peer_5", 12349)
]

# estado global do peer, variaveis compartilhadas entre threads
lock = threading.Lock() # garante que apenas uma thread acesse as variaveis compartilhadas por vez
pending_requests = [] # fila de espera global, armazena tupla de todas as requisicoes pendentes no sistema
my_current_request = None # requisicao atual do peer, se ele nao estiver na fila, sera None
ok_responses_received = 0 # contador de respostas OK recebidas para a requisicao atual
client_sockets_for_reply = {} # dicionario que mapeia client_id para o socket do cliente que deve receber a resposta
deferred_oks = {} # dicionario que armazena OKs que devem ser enviados mais tarde, requisicao do proprio peer eh mais prioritaria
peer_ready = threading.Event() # semaforo liberado quando todos os peers estao prontos, impedindo processamento de requisicoes antes do sistema estar estavel


def send_message(host, port, message, retries=5, initial_timeout=1.0):
    '''
    envia uma mensagem para um peer especificado por host e porta.
    retorna True se a mensagem foi enviada com sucesso, ou False se houve falha.
    '''
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
    '''
    envia uma mensagem para todos os peers do cluster, exceto o próprio peer.
    '''
    for peer_id, peer_host, peer_port in CLUSTER_MEMBERS:
        if peer_id != CLUSTER_ID:
            send_message(peer_host, peer_port, message)


def broadcast_request_to_peers(client_id, client_timestamp):
    '''
    define a requisicao atual, adiciona na propria fila de espera.
    envia uma mensagem de requisicao REQUEST_CLUSTER para todos os outros peers.
    '''
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
    '''
    funcao chamada quando cliente envia requisicao.
    guarda o socket do cliente para enviar resposta depois
    chama broadcast_request_to_peers para iniciar processo de votacao
    '''
    with lock:
        client_sockets_for_reply[message["client_id"]] = conn
    broadcast_request_to_peers(message["client_id"], message["timestamp"])


def handle_cluster_request(message):
    '''
    funcao executada quando outro peer envia uma requisicao REQUEST_CLUSTER
    adiciona a requisicao a sua fila local pending_requests e reordena a fila
    compara o timestamp da requisicao recebida com o timestamp da requisicao atual do peer
    se ele nao tem requisicao atual ou se o pedido recebido eh mais antigo, envia OK para o peer que fez a requisicao
    se nao, adia a resposta e adiciona o peer solicitante ao dicionario deferred_oks
    '''
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
    '''
    funcao chamada no peer original toda vez que recebe uma resposta OK de outro peer
    incrementa o ok_responses_received e quando chega ao numero de peers do cluster faz a verificacao final
    se a requisicao atual eh a primeira da fila, chama execute_critical_section
    '''
    global ok_responses_received
    if target_peer_id != CLUSTER_ID: return
    with lock:
        if my_current_request is None: return
        ok_responses_received += 1
        print(f"[{CLUSTER_ID}] Recebeu OK de {responding_peer_id}. Total: {ok_responses_received}/{len(CLUSTER_MEMBERS)}.")
        if ok_responses_received == len(CLUSTER_MEMBERS) and pending_requests[0] == my_current_request:
            execute_critical_section()


def handle_release_message(message):
    '''
    funcao executada em todos os peers quando recebem a mensagem RELEASE de outro peer que liberou o recurso
    simplesmente remove a requisicao finalizada da sua fila de pending_requests
    apos remover o item, ela checa se por acaso agora eh a sua vez de entrar na secao critica (caso ja tenha os 5 ok)
    '''
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
    '''
    funcao chamada quando um peer ganha o direito de acesso ao recurso compartilhado
    '''
    global pending_requests, my_current_request, ok_responses_received, deferred_oks


    print(f"[{CLUSTER_ID}] *** SEÇÃO CRÍTICA INICIADA para {my_current_request[1]} ***")
    
    # prepara dados para I/O antes de modificar o estado
    req_tuple = my_current_request
    client_socket = client_sockets_for_reply.pop(req_tuple[1], None)
    
    # modifica o estado atomicamente com lock
    # remove sua requisicao da fila de espera, limpa variaveis de estado e pega lista de oks adiados para enviar depois
    pending_requests.pop(0)
    my_current_request = None
    ok_responses_received = 0
    peers_to_send_ok = list(deferred_oks.keys())
    deferred_oks.clear()
    
    # simula o trabalho na secao critica
    sleep_time = random.uniform(0.2, 1)
    print(f"[{CLUSTER_ID}] ...Trabalhando por {sleep_time:.2f}s...")
    time.sleep(sleep_time)
    
    # envia COMMITTED para o cliente que fez a requisicao
    if client_socket:
        try:
            client_socket.sendall(b"COMMITTED")
            print(f"[{CLUSTER_ID}] Enviou COMMITTED para {req_tuple[1]}.")
        except Exception as e: print(f"[{CLUSTER_ID}] Erro ao enviar COMMITTED: {e}")
        finally: client_socket.close()

    print(f"[{CLUSTER_ID}] *** SEÇÃO CRÍTICA LIBERADA ***")

    # transmite uma mensagem RELEASE para todos os peers, sinalizando que terminou e o recurso esta livre para uso
    release_msg = { "type": "RELEASE", "releasing_peer_id": CLUSTER_ID,
                    "client_id": req_tuple[1], "client_timestamp": req_tuple[0] }
    broadcast_to_peers(release_msg)

    # envia as mensagens OK que estavam na sua lista deferred_oks
    for peer_id in peers_to_send_ok:
        target_host, target_port = next(((p[1], p[2]) for p in CLUSTER_MEMBERS if p[0] == peer_id), (None, None))
        if target_host:
            ok_msg = {"type": "OK", "responding_peer_id": CLUSTER_ID, "target_peer_id": peer_id}
            send_message(target_host, target_port, ok_msg)


def handle_connection(conn, addr):
    '''
    lida com conexoes de clientes ou outros peers.
    le a mensagem recebuida, decodifica e olha o campo type no JSON.
    Com base no tipo, chama a funcao apropriada para processar a mensagem.
    '''
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
    '''
    verifica se todos os peers do cluster estao prontos para processar requisicoes.
    '''
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
    '''
    loop infinito que aceita conexoes de rede, para cada nova conexao ele cria uma thread 
    para executar handle_connection.
    Assim, o servidor atende multiplas conexoes simultaneamente.
    '''
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
    # duas threads: uma inicia o servidor TCP qe fica escutando para sempre por conexoes de clientes ou outros peers
    # outra executa a verificacao que garante que todos os nós estao de pe, permitindo que o sistema comece a processar requisições
    server_thread = threading.Thread(target=start_peer_server, daemon=True)
    readiness_thread = threading.Thread(target=check_cluster_readiness, daemon=True)
    server_thread.start()
    readiness_thread.start()
    try:
        readiness_thread.join()
        print(f"[{CLUSTER_ID}] Cluster pronto. Peer operacional.")
        server_thread.join()
    except KeyboardInterrupt: print(f"\n[{CLUSTER_ID}] Encerrando.")