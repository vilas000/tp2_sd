import socket
import threading
import time
import random
import datetime
import json # Para serializar e desserializar mensagens complexas
import os

# --- Configurações Iniciais ---
# Lendo variáveis de ambiente definidas no docker-compose.yml
CLUSTER_ID = os.getenv("PEER_ID", f"Peer_{random.randint(1000, 9999)}")
HOST = '0.0.0.0' # O contêiner sempre escuta em 0.0.0.0 para ser acessível

# Mapeamento fixo de IDs de peers para portas.
# IMPORTANTE: Garanta que estas portas correspondem às portas internas no seu docker-compose.yml
PORT_MAP = {
    "Peer_1": 12345,
    "Peer_2": 12346,
    "Peer_3": 12347,
    "Peer_4": 12348,
    "Peer_5": 12349
}
PORT = PORT_MAP[CLUSTER_ID] # Pega a porta específica para este PEER_ID

# Lista de todos os membros do Cluster Sync (ID, HOSTNAME_DOCKER, PORT)
# Os HOSTNAME_DOCKER são os nomes dos serviços definidos no docker-compose.yml
CLUSTER_MEMBERS = [
    ("Peer_1", "sync_peer_1", 12345),
    ("Peer_2", "sync_peer_2", 12346),
    ("Peer_3", "sync_peer_3", 12347),
    ("Peer_4", "sync_peer_4", 12348),
    ("Peer_5", "sync_peer_5", 12349)
]

# --- Estado do Peer ---
# Bloqueio para acesso seguro aos recursos compartilhados entre threads
lock = threading.Lock()

# Fila de requisições pendentes (timestamp, client_id, requesting_peer_id)
# O client_address_for_reply é guardado separadamente agora.
pending_requests = []

# Requisição atual que este peer está tentando processar
# (timestamp_da_minha_req, client_id_original, meu_cluster_id)
my_current_request = None

# Número de OKs recebidos para my_current_request
ok_responses_received = 0

# Dicionário para armazenar o ENDEREÇO do cliente original para resposta final
# { "CLIENT_ID": (client_host, client_port) }
client_addresses_for_reply = {}

# Adicione uma lista para armazenar peers que devem receber OKs atrasados
deferred_oks = {} # { requesting_peer_id: [(client_timestamp, client_id)], ... }

# --- Funções Auxiliares ---

def get_current_timestamp():
    """Gera um timestamp no formato ISO para fácil comparação."""
    return datetime.datetime.now().isoformat()

def send_message(host, port, message):
    """Envia uma mensagem JSON para um host:port."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5) # Define um timeout para a conexão
            s.connect((host, port))
            s.sendall(json.dumps(message).encode('utf-8'))
            # print(f"[{CLUSTER_ID}] Enviou: {message['type']} para {host}:{port}") # Opcional para depuração mais detalhada
    except socket.timeout:
        print(f"[{CLUSTER_ID}] Timeout ao conectar para {host}:{port}")
    except ConnectionRefusedError:
        print(f"[{CLUSTER_ID}] Erro: Conexão recusada ao enviar para {host}:{port}. O peer pode não estar pronto.")
    except Exception as e:
        print(f"[{CLUSTER_ID}] Erro ao enviar mensagem para {host}:{port}: {e}")

def broadcast_request_to_peers(client_id, client_timestamp, client_addr):
    """
    Broadcasta a requisição do cliente para todos os membros do cluster.
    Inclui a si mesmo para processamento local.
    """
    global my_current_request, ok_responses_received

    request_msg = {
        "type": "REQUEST_CLUSTER",
        "requester_peer_id": CLUSTER_ID,
        "client_id": client_id,
        "client_timestamp": client_timestamp
    }

    with lock:
        # Só inicializa my_current_request se não estiver processando uma
        # Se um cliente envia múltiplas requests para o mesmo peer,
        # este peer só pode processar uma por vez dentro do protocolo.
        if my_current_request is None:
            my_current_request = (client_timestamp, client_id, CLUSTER_ID)
            # A porta para o cliente foi salva em handle_client_request, mas aqui
            # garantimos que ela esteja mapeada para este my_current_request.
            client_addresses_for_reply[client_id] = client_addr
            ok_responses_received = 0 # Reinicia o contador para a nova requisição
            print(f"[{CLUSTER_ID}] Minha requisição atual para {client_id}: {my_current_request}")
        else:
            # Se já estiver processando uma requisição minha, apenas ignora esta nova
            # do mesmo cliente ou de outro. Você pode enfileirar aqui se quiser lidar
            # com múltiplas requisições simultâneas por peer.
            print(f"[{CLUSTER_ID}] Já estou processando uma requisição ({my_current_request}). Ignorando a nova de {client_id}.")
            return # Sai da função para não enviar broadcasts desnecessários

    # Envia para todos os peers, incluindo ele mesmo (para processamento uniforme)
    for peer_id, peer_host, peer_port in CLUSTER_MEMBERS:
        if peer_id == CLUSTER_ID:
            # Processa a própria requisição localmente para contar o 'OK' de si mesmo
            with lock:
                if my_current_request and \
                   my_current_request[0] == client_timestamp and \
                   my_current_request[1] == client_id: # Confirma que é a minha requisição
                    
                    ok_responses_received += 1
                    print(f"[{CLUSTER_ID}] (Local) Recebeu OK de si mesmo. Total de OKs: {ok_responses_received}")
                    if ok_responses_received >= len(CLUSTER_MEMBERS):
                        print(f"[{CLUSTER_ID}] *** Meu turno! Entrando na SEÇÃO CRÍTICA (local) ***")
                        execute_critical_section()
        else:
            send_message(peer_host, peer_port, request_msg)

def handle_client_request(conn, addr, message): # Agora recebe 'message' (já JSON)
    """Processa uma requisição recebida de um cliente."""
    client_id = message["client_id"]
    client_timestamp = message["timestamp"]
    print(f"[{CLUSTER_ID}] Recebeu requisição de cliente: {client_id} com timestamp {client_timestamp}")

    # Salva o ENDEREÇO do cliente para responder depois
    with lock:
        client_addresses_for_reply[client_id] = addr
    
    # Inicia o processo de consenso no cluster
    broadcast_request_to_peers(client_id, client_timestamp, addr)
    conn.close() # Fecha a conexão de entrada do cliente AQUI, pois a resposta será por um novo socket.


def handle_cluster_request(message):
    """Processa uma requisição recebida de outro membro do cluster (ou de si mesmo)."""
    global pending_requests, my_current_request, deferred_oks

    req_peer_id = message["requester_peer_id"]
    client_id = message["client_id"]
    client_timestamp = message["client_timestamp"]

    with lock:
        # Adiciona a requisição à fila de pendentes
        new_request_tuple = (client_timestamp, client_id, req_peer_id)
        # Adiciona a requisição à fila de pendentes
        # Evita duplicatas se o mesmo peer enviar múltiplas requests para o mesmo cliente rapidamente
        if new_request_tuple not in pending_requests: # Evita duplicatas
            pending_requests.append(new_request_tuple)
            pending_requests.sort() # Mantém a fila ordenada por timestamp

        print(f"[{CLUSTER_ID}] Adicionou requisição do Peer {req_peer_id} para {client_id} (ts: {client_timestamp}). Fila: {pending_requests}")


        # Regra de votação:
        # Lógica de Votação Ricart-Agrawala:
        # Se eu não tenho uma requisição ou a recebida é mais antiga, eu voto OK.
        # Caso contrário, eu enfileiro o OK para ser enviado depois.
        if my_current_request is None or client_timestamp < my_current_request[0] or \
           (client_timestamp == my_current_request[0] and req_peer_id < my_current_request[2]):
            
            # Se a requisição recebida é a minha, não precisamos enviar OK via rede
            if req_peer_id == CLUSTER_ID:
                # O 'OK' local já é contado na broadcast_request_to_peers
                pass 
            else:
                ok_msg = {
                    "type": "OK",
                    "responding_peer_id": CLUSTER_ID,
                    "target_peer_id": req_peer_id
                }
                target_host, target_port = next((p[1], p[2]) for p in CLUSTER_MEMBERS if p[0] == req_peer_id)
                send_message(target_host, target_port, ok_msg)
                print(f"[{CLUSTER_ID}] Enviou OK imediato para {req_peer_id} para requisição de {client_id}")
        else:
            # Enfileira o OK para ser enviado APÓS a seção crítica
            if req_peer_id != CLUSTER_ID: # Não enfileira OK para si mesmo
                if req_peer_id not in deferred_oks:
                    deferred_oks[req_peer_id] = []
                deferred_oks[req_peer_id].append((client_timestamp, client_id))
                print(f"[{CLUSTER_ID}] Enfileirou OK para {req_peer_id} para requisição de {client_id}. Meu timestamp ({my_current_request[0]}) é anterior/igual.")


def process_ok_response(responding_peer_id, target_peer_id):
    """Processa uma resposta OK recebida de outro membro do cluster."""
    global ok_responses_received, my_current_request

    # Este OK é para a minha requisição atual?
    if target_peer_id != CLUSTER_ID:
        print(f"[{CLUSTER_ID}] Recebeu OK de {responding_peer_id} que não é para mim ({target_peer_id}).")
        return 

    with lock:
        if my_current_request is not None and my_current_request[2] == CLUSTER_ID: # Confirma que é meu pedido
            ok_responses_received += 1
            print(f"[{CLUSTER_ID}] Recebeu OK de {responding_peer_id}. Total de OKs: {ok_responses_received}")

            # Se todos os OKs foram recebidos, entrar na seção crítica
            if ok_responses_received >= len(CLUSTER_MEMBERS): # Todos os OKs (incluindo o meu local)
                print(f"[{CLUSTER_ID}] *** TENHO TODOS OS OKs! Entrando na SEÇÃO CRÍTICA ***")
                execute_critical_section()
        else:
            print(f"[{CLUSTER_ID}] Recebeu OK de {responding_peer_id} para uma requisição que não é a minha atual ou já foi processada.")

def execute_critical_section():
    """
    Lógica da seção crítica, envio de COMMITTED ao cliente original
    e liberação do recurso, incluindo o envio de OKs atrasados.
    """
    global my_current_request, ok_responses_received, pending_requests, deferred_oks

    # Garante que há uma requisição para processar
    if my_current_request is None:
        print(f"[{CLUSTER_ID}] Erro: Tentativa de entrar na seção crítica sem my_current_request.")
        return

    client_id_to_reply = my_current_request[1]
    
    # Recupera o endereço do cliente salvo para responder
    client_addr = None
    with lock:
        # Pega o endereço e remove da fila de espera por resposta, já que será respondido agora
        if client_id_to_reply in client_addresses_for_reply:
            client_addr = client_addresses_for_reply.pop(client_id_to_reply)

    if client_addr:
        # --- SEÇÃO CRÍTICA REAL (simulada) ---
        sleep_time = random.uniform(0.2, 1.0)
        print(f"[{CLUSTER_ID}] Escrevendo no Recurso R por {sleep_time:.2f}s...")
        time.sleep(sleep_time)
        print(f"[{CLUSTER_ID}] Escrita no Recurso R concluída.")
        # --- FIM DA SEÇÃO CRÍTICA REAL ---

        # Envia COMMITTED para o cliente original que solicitou o acesso
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5) # Timeout para a conexão de resposta
                s.connect(client_addr)
                s.sendall(b"COMMITTED")
                print(f"[{CLUSTER_ID}] Enviou COMMITTED para cliente {client_id_to_reply} em {client_addr}")
        except Exception as e:
            print(f"[{CLUSTER_ID}] Erro ao enviar COMMITTED para {client_id_to_reply} em {client_addr}: {e}")
    else:
        print(f"[{CLUSTER_ID}] Aviso: Não encontrou endereço do cliente {client_id_to_reply} para responder. Ele pode ter desconectado.")
        
    # --- LIBERAÇÃO DA SEÇÃO CRÍTICA E ENVIO DE OKS ATRASADOS ---
    with lock:
        # Remove a requisição que acabou de ser processada da fila de pendentes
        # Isso é crucial para que o Ricart-Agrawala funcione, removendo a requisição
        # que acabou de ser atendida.
        pending_requests = [req for req in pending_requests
                            if not (req[0] == my_current_request[0] and 
                                    req[1] == my_current_request[1] and 
                                    req[2] == my_current_request[2])]

        # Reinicia o estado deste peer para que ele possa processar uma nova requisição
        my_current_request = None
        ok_responses_received = 0
        print(f"[{CLUSTER_ID}] Seção crítica liberada. Estado resetado. Fila atual: {pending_requests}")

        # Envia todos os OKs que foram enfileirados enquanto este peer estava na SC
        # Para cada peer que tem requisições esperando meu OK
        peers_to_clear_deferred = list(deferred_oks.keys()) # Crie uma cópia da chave para iterar
        for peer_id_to_ok in peers_to_clear_deferred:
            # Encontra o host e porta do peer que deve receber o OK
            target_host, target_port = next((p[1], p[2]) for p in CLUSTER_MEMBERS if p[0] == peer_id_to_ok)
            
            # Envia um OK para cada requisição enfileirada para este peer
            # A mensagem OK não precisa dos detalhes da requisição, apenas o target_peer_id
            ok_msg = {
                "type": "OK",
                "responding_peer_id": CLUSTER_ID,
                "target_peer_id": peer_id_to_ok
            }
            send_message(target_host, target_port, ok_msg)
            print(f"[{CLUSTER_ID}] Enviou OK atrasado para {peer_id_to_ok}.")
        
        deferred_oks.clear() # Limpa a lista de OKs enfileirados após enviá-los todos



def handle_connection(conn, addr):
    """Lida com uma conexão de entrada (cliente ou outro peer)."""
    try:
        data = conn.recv(4096).decode('utf-8') # Aumentado o buffer para mensagens JSON
        
        # Tenta desserializar como JSON
        message = json.loads(data)

        if message.get("type") == "REQUEST": # Mensagem de um cliente
            handle_client_request(conn, addr, message)
        elif message.get("type") == "REQUEST_CLUSTER": # Mensagem de um peer
            handle_cluster_request(message)
        elif message.get("type") == "OK": # Mensagem de OK de um peer
            process_ok_response(message["responding_peer_id"], message["target_peer_id"])
        else:
            print(f"[{CLUSTER_ID}] Mensagem desconhecida recebida de {addr}: {message}")

    except json.JSONDecodeError:
        print(f"[{CLUSTER_ID}] Erro de JSON: Recebeu mensagem não JSON de {addr}: {data}")
        # Opcional: Responder com erro ao cliente se a mensagem não for JSON,
        # mas para este cenário, um log já é suficiente.
    except Exception as e:
        print(f"[{CLUSTER_ID}] Erro ao lidar com conexão de {addr}: {e}")
    finally:
        conn.close() # Sempre fechar a conexão de entrada


def start_peer_server():
    """Inicia o servidor de escuta para este membro do Cluster Sync."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Permite reusar o endereço imediatamente
        s.bind((HOST, PORT))
        s.listen()
        print(f"[{CLUSTER_ID}] Servidor escutando em {HOST}:{PORT}")

        while True:
            try:
                conn, addr = s.accept()
                print(f"[{CLUSTER_ID}] Conexão aceita de {addr}")
                thread = threading.Thread(target=handle_connection, args=(conn, addr))
                thread.daemon = True # Permite que a thread termine com o programa principal
                thread.start()
            except Exception as e:
                print(f"[{CLUSTER_ID}] Erro no loop de aceitação de conexão: {e}")

# --- Função Principal ---

if __name__ == "__main__":
    print(f"[{CLUSTER_ID}] Meu ID: {CLUSTER_ID}, Meu Endereço: {HOST}:{PORT}")
    print(f"[{CLUSTER_ID}] Membros do Cluster Conhecidos: {CLUSTER_MEMBERS}")

    # Inicia o servidor em uma thread separada para permitir outras operações (como manter o programa vivo)
    server_thread = threading.Thread(target=start_peer_server)
    server_thread.daemon = True # Torna-a uma thread daemon
    server_thread.start()

    # Mantém a thread principal viva para que as threads daemon possam continuar rodando
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"[{CLUSTER_ID}] Peer encerrado.")