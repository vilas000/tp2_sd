# cluster_sync_app/cluster_sync_peer.py

import socket
import threading
import time
import random
import json
import os

# configuracoes iniciais do peer, serao lidos das variaveis de ambiente do docker compose
CLUSTER_ID = os.getenv("PEER_ID", f"Peer_{random.randint(1000, 9999)}")
HOST = '0.0.0.0'
PORT = int(os.getenv("PEER_PORT"))

# lista de peers do cluster para que cada peer saiba quem sao os outros
ALL_CLUSTER_MEMBERS = [
    ("Peer_1", "sync_peer_1", 12345), ("Peer_2", "sync_peer_2", 12346),
    ("Peer_3", "sync_peer_3", 12347), ("Peer_4", "sync_peer_4", 12348),
    ("Peer_5", "sync_peer_5", 12349)
]

# estado global do peer, variaveis compartilhadas entre threads
lock = threading.Lock() # garante que apenas uma thread acesse as variaveis compartilhadas por vez
pending_requests = [] # fila de espera global, armazena tupla de todas as requisicoes pendentes no sistema
managed_requests = {} # Dicionário para gerenciar o estado de cada requisição que ESTE peer é responsável
deferred_oks = {} # dicionario que armazena OKs que devem ser enviados mais tarde, requisicao do proprio peer eh mais prioritaria
client_sockets_for_reply = {} # dicionario que mapeia client_id para o socket do cliente que deve receber a resposta
active_cluster_members = list(ALL_CLUSTER_MEMBERS) # lista de membros ativos do cluster, inicia com todos os peers conhecidos
peer_ready = threading.Event() # semaforo liberado quando todos os peers estao prontos, impedindo processamento de requisicoes antes do sistema estar estavel


def handle_peer_failure(failed_peer_tuple):
    '''
    trata a falha de um peer, removendo-o da lista de membros ativos e verificando se pode entrar na seção crítica.
    '''
    global active_cluster_members
    with lock:
        if failed_peer_tuple in active_cluster_members: # Verifica se o peer que falhou está na lista de membros ativos
            active_cluster_members.remove(failed_peer_tuple) # Remove o peer que falhou da lista de membros ativos
            print(f"[{CLUSTER_ID}] DETECTOU FALHA: Peer {failed_peer_tuple[0]} removido. Membros ativos: {len(active_cluster_members)}.")
    check_if_can_enter_critical_section() # Verifica se pode entrar na seção crítica após a remoção do peer falho


def send_message(host, port, message, retries=3, initial_timeout=0.5):
    '''
    envia uma mensagem para um peer especificado por host e porta, com retries e timeout configuráveis.
    retorna a resposta se for um READY_CHECK, ou True/False se foi enviado.
    '''
    try:
        with socket.create_connection((host, port), timeout=initial_timeout): # cria uma conexão com o peer especificado
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: 
                s.settimeout(initial_timeout) 
                s.connect((host, port)) 
                s.sendall(json.dumps(message).encode('utf-8'))
                if message.get("type") == "READY_CHECK": # se a mensagem for um READY_CHECK, espera pela resposta
                    s.recv(1024)
                return True
    except (OSError, socket.timeout):
        return False


def broadcast_to_peers(message):
    '''
    envia uma mensagem para todos os peers do cluster, exceto o próprio peer.
    '''
    with lock: 
        current_active_peers = list(active_cluster_members) # copia a lista de membros ativos do cluster para evitar problemas de concorrência

    for peer_tuple in current_active_peers: # verifica cada peer ativo
        if peer_tuple[0] != CLUSTER_ID: # não envia mensagem para si mesmo
            if not send_message(peer_tuple[1], peer_tuple[2], message): # se falhar ao enviar a mensagem, trata a falha do peer
                handle_peer_failure(peer_tuple)


def handle_client_request(conn, addr, message):
    ''' 
    trata a requisição do cliente, verificando se já existe uma requisição duplicada ou pendente e gerenciando o estado da requisição no cluster. 
    '''
    global pending_requests, managed_requests, client_sockets_for_reply
    client_id = message["client_id"] # ID do cliente que fez a requisição
    timestamp = message["timestamp"] # timestamp da requisição do cliente
    req_tuple = (timestamp, client_id, CLUSTER_ID) # tupla que representa a requisição, incluindo o ID do peer que está gerenciando a requisição
    req_key = (timestamp, client_id) # chave para identificar a requisição no dicionário de gerenciadas
 
    with lock:
        request_exists = any(req[0] == timestamp and req[1] == client_id for req in pending_requests) # verifica se já existe uma requisição pendente com o mesmo timestamp e client_id
        if request_exists:
            print(f"[{CLUSTER_ID}] Requisição duplicada de {client_id}. Respondendo PROCESSING.") 
            try:
                conn.sendall(b"PROCESSING")
            except Exception as e:
                print(f"[{CLUSTER_ID}] Erro ao enviar PROCESSING: {e}")
            finally:
                conn.close()
            return
        
        print(f"[{CLUSTER_ID}] Iniciando nova requisição para {client_id}.")
        pending_requests.append(req_tuple) # adiciona a requisição à fila de pendentes
        pending_requests.sort() # ordena a fila de pendentes por timestamp e client_id
        managed_requests[req_key] = {"ok_count": 1, "status": "pending"} # inicializa o estado da requisição gerenciada
        client_sockets_for_reply[req_key] = conn # armazena o socket do cliente para enviar a resposta posteriormente
    
    broadcast_msg = { "type": "REQUEST_CLUSTER", "requester_peer_id": CLUSTER_ID, "client_id": client_id, "client_timestamp": timestamp } # mensagem de requisição para o cluster
    broadcast_to_peers(broadcast_msg) # envia a mensagem de requisição para todos os peers do cluster
    check_if_can_enter_critical_section() # verifica se pode entrar na seção crítica


def handle_cluster_request(message):
    '''
    trata a requisição de cluster, verificando se o peer pode responder imediatamente ou se deve adiar a resposta.
    '''
    global pending_requests, deferred_oks 
    req_peer_id = message["requester_peer_id"] # ID do peer que fez a requisição
    client_id = message["client_id"] # ID do cliente que fez a requisição
    timestamp = message["client_timestamp"] # timestamp da requisição do cliente
    new_request_tuple = (timestamp, client_id, req_peer_id) # tupla que representa a nova requisição, incluindo o ID do peer que está gerenciando a requisição

    with lock:
        if new_request_tuple not in pending_requests: # verifica se a nova requisição já está na fila de pendentes
            pending_requests.append(new_request_tuple) # adiciona a nova requisição à fila de pendentes
            pending_requests.sort() # ordena a fila de pendentes por timestamp e client_id
        
        my_managed_requests = [key for key, val in managed_requests.items() if val.get("status") == "pending"] # filtra as requisições pendentes gerenciadas por este peer
        should_defer = False # verifica se deve adiar a resposta
        for my_req_key in my_managed_requests:
            my_req_tuple = (my_req_key[0], my_req_key[1], CLUSTER_ID) # cria uma tupla de requisição para comparar
            if my_req_tuple < new_request_tuple: # se a requisição do peer é mais antiga que uma pendente gerenciada
                should_defer = True # se deve adiar a resposta
                break
        
        if not should_defer: 
            target_tuple = next((p for p in ALL_CLUSTER_MEMBERS if p[0] == req_peer_id), None) # encontra o peer alvo na lista de membros do cluster
            if target_tuple:
                ok_msg = {"type": "OK", "responding_peer_id": CLUSTER_ID, "target_peer_id": req_peer_id, "request_ts": timestamp, "request_client_id": client_id}
                if not send_message(target_tuple[1], target_tuple[2], ok_msg): # tenta enviar a mensagem OK para o peer alvo
                    handle_peer_failure(target_tuple)
        else:
            deferred_oks[new_request_tuple] = True # adia o envio do OK para esta requisição


def process_ok_response(message):
    '''
    processa a resposta OK recebida de outro peer, atualizando o contador de OKs e verificando se pode entrar na seção crítica.
    '''
    global managed_requests
    req_key = (message["request_ts"], message["request_client_id"]) # cria uma chave para a requisição recebida
    
    with lock:
        if req_key in managed_requests: # se a requisição está sendo gerenciada por este peer
            managed_requests[req_key]["ok_count"] += 1 # incrementa o contador de OKs
            num_active = len(active_cluster_members) # número de membros ativos no cluster
            print(f"[{CLUSTER_ID}] Recebeu OK de {message['responding_peer_id']} para {req_key[1]}. Total: {managed_requests[req_key]['ok_count']}/{num_active}.")
    check_if_can_enter_critical_section() # Verifica se pode entrar na seção crítica após receber o OK


def check_if_can_enter_critical_section():
    '''
    verifica se o peer pode entrar na seção crítica, analisando a fila de requisições pendentes e o estado das requisições gerenciadas.
    '''
    global managed_requests
    with lock:
        if not pending_requests: return # se não há requisições pendentes, não faz nada
        head_of_queue = pending_requests[0]
        req_key = (head_of_queue[0], head_of_queue[1]) # cria uma chave para a requisição na cabeça da fila
        
        if head_of_queue[2] == CLUSTER_ID: # Se este peer é o responsável pela requisição na cabeça da fila
            if req_key in managed_requests and managed_requests[req_key]["status"] == "pending": # verifica se a requisição está pendente
                num_active = len(active_cluster_members) # número de membros ativos no cluster
                if managed_requests[req_key]["ok_count"] >= num_active: # se o número de OKs recebidos é suficiente
                    managed_requests[req_key]["status"] = "in_cs" # atualiza o status da requisição para "in_cs"(dentro da seção crítica)
                    threading.Thread(target=execute_critical_section, args=(head_of_queue,)).start() # inicia a execução da seção crítica em uma nova thread


def execute_critical_section(req_tuple):
    '''
    executa a seção crítica, enviando a resposta COMMITTED ao cliente e liberando a seção crítica.
    '''
    global pending_requests, client_sockets_for_reply, managed_requests, deferred_oks
    req_key = (req_tuple[0], req_tuple[1]) # cria uma chave para a requisição
    
    print(f"[{CLUSTER_ID}] *** SEÇÃO CRÍTICA INICIADA para {req_key[1]} ***")
    time.sleep(random.uniform(0.2, 1)) # simula o tempo de execução da seção crítica

    client_socket = client_sockets_for_reply.pop(req_key, None) # obtém o socket do cliente para enviar a resposta
    if client_socket: # se o socket do cliente está disponível
        try: 
            client_socket.sendall(b"COMMITTED") # envia a resposta COMMITTED ao cliente
            print(f"[{CLUSTER_ID}] Enviou COMMITTED para {req_key[1]}.")
        except Exception as e: print(f"[{CLUSTER_ID}] Erro ao enviar COMMITTED: {e}")
        finally: client_socket.close()

    print(f"[{CLUSTER_ID}] *** SEÇÃO CRÍTICA LIBERADA ***") # libera a seção crítica
    # Envia mensagem de liberação para todos os peers, informando que a seção crítica foi liberada
    
    release_msg = {"type": "RELEASE", "releasing_peer_id": CLUSTER_ID, "client_id": req_key[1], "client_timestamp": req_key[0]}
    broadcast_to_peers(release_msg) # envia a mensagem de liberação para todos os peers
    
    with lock:
        if req_tuple in pending_requests: pending_requests.remove(req_tuple) # remove a requisição da fila de pendentes
        managed_requests.pop(req_key, None) # remove a requisição do dicionário de gerenciadas
        
        oks_to_send_now = [] # lista de OKs que devem ser enviados agora
        remaining_deferred = {} # dicionário para armazenar OKs que devem ser enviados mais tarde
        for deferred_req, _ in deferred_oks.items(): # verifica as requisições adiadas
            oks_to_send_now.append(deferred_req) # adiciona as requisições adiadas que devem ser enviadas agora
        deferred_oks = remaining_deferred # limpa o dicionário de OKs adiados
    
    for deferred_req_tuple in oks_to_send_now: # envia os OKs adiados agora
        ok_msg = {"type": "OK", "responding_peer_id": CLUSTER_ID, "target_peer_id": deferred_req_tuple[2], "request_ts": deferred_req_tuple[0], "request_client_id": deferred_req_tuple[1]}
        target_info = next((p for p in ALL_CLUSTER_MEMBERS if p[0] == deferred_req_tuple[2]), None) # encontra o peer alvo na lista de membros do cluster
        if target_info:
            if not send_message(target_info[1], target_info[2], ok_msg): # tenta enviar a mensagem OK para o peer alvo
                handle_peer_failure(target_info)
    
    check_if_can_enter_critical_section() # Verifica se há outras requisições pendentes que podem ser processadas


def handle_release_message(message):
    '''
    trata a mensagem de liberação, removendo a requisição pendente e verificando se pode entrar na seção crítica.
    '''
    global pending_requests
    request_to_remove = (message["client_timestamp"], message["client_id"], message["releasing_peer_id"])
    with lock:
        if request_to_remove in pending_requests:
            pending_requests.remove(request_to_remove) # remove a requisição da fila de pendentes
    check_if_can_enter_critical_section() # Verifica se há outras requisições pendentes que podem ser processadas


def handle_connection(conn, addr):
    '''
    trata a conexão recebida, decodificando a mensagem e encaminhando para o manipulador apropriado.
    '''
    try:
        data = conn.recv(1024) # recebe os dados do cliente
        if not data: conn.close(); return # se não houver dados, fecha a conexão
        message = json.loads(data.decode('utf-8')) # decodifica a mensagem recebida
        msg_type = message.get("type") # obtém o tipo da mensagem

        if msg_type == "REQUEST": peer_ready.wait(); handle_client_request(conn, addr, message) # trata a requisição do cliente
        else:
            if msg_type == "REQUEST_CLUSTER": handle_cluster_request(message) # trata a requisição de cluster
            elif msg_type == "OK": process_ok_response(message) # processa a resposta OK recebida de outro peer
            elif msg_type == "RELEASE": handle_release_message(message) # trata a mensagem de liberação
            elif msg_type == "READY_CHECK": conn.sendall(b"ACK_READY") # responde ao READY_CHECK com ACK_READY
            conn.close() # fecha a conexão após o processamento
    except Exception: conn.close() # fecha a conexão em caso de erro


def check_cluster_readiness():
    '''
    verifica a prontidão do cluster, aguardando que todos os peers estejam online antes de processar requisições.
    '''
    print(f"[{CLUSTER_ID}] Verificando prontidão do cluster...")
    ready_peers = {CLUSTER_ID} # inicia com o próprio peer como pronto
    while len(ready_peers) < len(ALL_CLUSTER_MEMBERS): # verifica se todos os peers estão prontos
        for peer_id, peer_host, peer_port in ALL_CLUSTER_MEMBERS:
            if peer_id not in ready_peers and send_message(peer_host, peer_port, {"type": "READY_CHECK"}): # envia um READY_CHECK para o peer
                ready_peers.add(peer_id) # adiciona o peer à lista de prontos
                print(f"[{CLUSTER_ID}] Peer {peer_id} está pronto. ({len(ready_peers)}/{len(ALL_CLUSTER_MEMBERS)})")
        time.sleep(1)
    print(f"[{CLUSTER_ID}] TODOS os peers estão online!"); peer_ready.set()


def start_peer_server():
    '''
    inicia o servidor do peer, escutando conexões e tratando requisições.
    '''
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind((HOST, PORT)); s.listen() # escuta conexões na porta especificada
        print(f"[{CLUSTER_ID}] Servidor escutando em {HOST}:{PORT}")
        while True:
            conn, addr = s.accept() # aceita uma conexão
            threading.Thread(target=handle_connection, args=(conn, addr), daemon=True).start() # inicia uma nova thread para tratar a conexão recebida


if __name__ == "__main__":
    '''
    Inicia o servidor e a verificação de prontidão do cluster em threads separadas.
    '''
    server_thread = threading.Thread(target=start_peer_server, daemon=True) # inicia o servidor em uma thread separada
    readiness_thread = threading.Thread(target=check_cluster_readiness, daemon=True) # inicia a verificação de prontidão do cluster em outra thread
    server_thread.start(); readiness_thread.start() # aguarda que o servidor e a verificação de prontidão estejam prontos
    try:
        readiness_thread.join(); print(f"[{CLUSTER_ID}] Cluster pronto. Peer operacional."); server_thread.join() # aguarda que o servidor esteja pronto
    except KeyboardInterrupt: print(f"\n[{CLUSTER_ID}] Encerrando.")