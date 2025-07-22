# import socket
# import threading
# import time
# import random
# import datetime
# import json # Para serializar e desserializar mensagens complexas
# import os

# # --- Configurações Iniciais ---
# # Lendo variáveis de ambiente definidas no docker-compose.yml
# CLUSTER_ID = os.getenv("PEER_ID", f"Peer_{random.randint(1000, 9999)}")# HOST = '127.0.0.1' # Pode ser '0.0.0.0' para containers/VMs se a rede for diferente
# HOST = '0.0.0.0' # O contêiner sempre escuta em 0.0.0.0 para ser acessível
# # A porta é a mesma que foi mapeada no docker-compose.yml e será passada para os clientes.
# # Usamos uma porta fixa para cada peer para facilitar a configuração dos clientes.
# PORT_MAP = {
#     "Peer_1": 12345,
#     "Peer_2": 12346,
#     "Peer_3": 12347,
#     "Peer_4": 12348,
#     "Peer_5": 12349
# }
# PORT = PORT_MAP[CLUSTER_ID] # Pega a porta específica para este PEER_ID

# # Lista de todos os membros do Cluster Sync (ID, HOSTNAME_DOCKER, PORT)
# # Os HOSTNAME_DOCKER são os nomes dos serviços definidos no docker-compose.yml
# CLUSTER_MEMBERS = [
#     ("Peer_1", "sync_peer_1", 12345),
#     ("Peer_2", "sync_peer_2", 12346),
#     ("Peer_3", "sync_peer_3", 12347),
#     ("Peer_4", "sync_peer_4", 12348),
#     ("Peer_5", "sync_peer_5", 12349)
# ]

# # --- Estado do Peer ---
# # Bloqueio para acesso seguro aos recursos compartilhados entre threads
# lock = threading.Lock()

# # Fila de requisições pendentes (timestamp, client_id, requesting_peer_id, client_address_for_reply)
# # client_address_for_reply: (host, port) do socket do cliente original
# pending_requests = []

# # Requisição atual que este peer está tentando processar
# # (timestamp_da_minha_req, client_id_original, meu_cluster_id)
# my_current_request = None

# # Número de OKs recebidos para my_current_request
# ok_responses_received = 0

# # Dicionário para armazenar o socket do cliente original para resposta
# # { "CLIENT_ID": client_socket_object }
# # CUIDADO: Passar objetos socket entre threads exige mais cuidado ou design diferente
# # Uma alternativa é guardar o endereço do cliente (host, port) e criar um novo socket para responder
# client_sockets_for_reply = {}

# # --- Funções Auxiliares ---

# def get_current_timestamp():
#     """Gera um timestamp no formato ISO para fácil comparação."""
#     return datetime.datetime.now().isoformat()

# def send_message(host, port, message):
#     """Envia uma mensagem JSON para um host:port."""
#     try:
#         # Tenta criar e conectar o socket
#         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#             s.settimeout(5) # Define um timeout para a conexão
#             s.connect((host, port))
#             s.sendall(json.dumps(message).encode('utf-8'))
#     except socket.timeout:
#         print(f"[{CLUSTER_ID}] Timeout ao conectar para {host}:{port}")
#     except ConnectionRefusedError:
#         print(f"[{CLUSTER_ID}] Erro: Conexão recusada ao enviar para {host}:{port}")
#     except Exception as e:
#         print(f"[{CLUSTER_ID}] Erro ao enviar mensagem para {host}:{port}: {e}")

# def broadcast_request_to_peers(client_id, client_timestamp, client_addr):
#     """
#     Broadcasta a requisição do cliente para todos os membros do cluster.
#     Inclui a si mesmo para processamento local.
#     """
#     global my_current_request, ok_responses_received

#     request_msg = {
#         "type": "REQUEST_CLUSTER",
#         "requester_peer_id": CLUSTER_ID,
#         "client_id": client_id,
#         "client_timestamp": client_timestamp
#     }

#     with lock:
#         # Se eu já tiver uma requisição minha, não sobrescrevo.
#         # Isso implica que só posso tentar uma requisição de cliente por vez.
#         # Se você permitir múltiplas, precisará de uma fila de requisições minhas também.
#         if my_current_request is None:
#             my_current_request = (client_timestamp, client_id, CLUSTER_ID)
#             # Armazena o endereço do cliente para a resposta final
#             client_sockets_for_reply[client_id] = client_addr
#             ok_responses_received = 0 # Reinicia o contador para a nova requisição
#             print(f"[{CLUSTER_ID}] Minha requisição atual: {my_current_request}")

#     # Envia para todos os peers, incluindo ele mesmo (para processamento uniforme)
#     for peer_id, peer_host, peer_port in CLUSTER_MEMBERS:
#         # Se for para mim mesmo, chamo a função de tratamento de mensagem diretamente
#         if peer_id == CLUSTER_ID:
#             # handle_cluster_request(request_msg)
#             # Processa a própria requisição localmente para contar o 'OK' de si mesmo
#             # sem enviar pela rede.
#             with lock:
#                 if my_current_request and my_current_request[0] == client_timestamp and my_current_request[1] == client_id:
#                     ok_responses_received += 1
#                     print(f"[{CLUSTER_ID}] (Local) Recebeu OK de si mesmo. Total de OKs: {ok_responses_received}")
#                     if ok_responses_received >= len(CLUSTER_MEMBERS):
#                         print(f"[{CLUSTER_ID}] *** Entrando na SEÇÃO CRÍTICA (local) ***")
#                         execute_critical_section()
#         else:
#             send_message(peer_host, peer_port, request_msg)

# def handle_client_request(client_socket, client_address, data):
#     """Processa uma requisição recebida de um cliente."""
#     parts = data.split('|')
#     if len(parts) == 3 and parts[0] == "REQUEST":
#         client_id = parts[1]
#         client_timestamp = parts[2]
#         print(f"[{CLUSTER_ID}] Recebeu requisição de cliente: {client_id} com timestamp {client_timestamp}")

#         # Guarda o socket do cliente para responder depois (ou o endereço, como preferir)
#         # Para simplificar, vou guardar o endereço e criar um novo socket para responder
#         with lock:
#             # Armazena o endereço do cliente para a resposta final
#             client_sockets_for_reply[client_id] = client_address

#         # Inicia o processo de consenso no cluster
#         broadcast_request_to_peers(client_id, client_timestamp, client_address)
#     else:
#         print(f"[{CLUSTER_ID}] Mensagem de cliente inválida: {data}")
#         # client_socket.sendall(b"INVALID_REQUEST")
#     # Não fecha o client_socket aqui se você for usar o socket salvo em client_sockets_for_reply
#     # Mas é mais seguro guardar o endereço e abrir um novo socket para a resposta COMMITTED.
#     client_socket.close() # Melhor fechar aqui e abrir novo para COMMITTED

# def handle_cluster_request(message):
#     """Processa uma requisição recebida de outro membro do cluster (ou de si mesmo)."""
#     global pending_requests, my_current_request

#     req_peer_id = message["requester_peer_id"]
#     client_id = message["client_id"]
#     client_timestamp = message["client_timestamp"]

#     with lock:
#         # Adiciona a requisição à fila de pendentes
#         pending_requests.append((client_timestamp, client_id, req_peer_id))
#         pending_requests.sort() # Mantém a fila ordenada por timestamp

#         print(f"[{CLUSTER_ID}] Adicionou requisição do Peer {req_peer_id} para {client_id} (ts: {client_timestamp}). Fila: {pending_requests}")

#         # Regra de votação:
#         # 1. Se eu não tiver uma requisição própria (my_current_request is None), OU
#         # 2. Se a requisição recebida tiver um timestamp estritamente MENOR que a minha
#         #    (e em caso de empate de timestamp, o ID do peer deve ser menor para desempate)
#         should_send_ok = False
#         if my_current_request is None:
#             should_send_ok = True
#         elif client_timestamp < my_current_request[0]:
#             should_send_ok = True
#         elif client_timestamp == my_current_request[0] and req_peer_id < my_current_request[2]: # Desempate por ID do peer
#              should_send_ok = True


#         if should_send_ok:
#             ok_msg = {
#                 "type": "OK",
#                 "responding_peer_id": CLUSTER_ID,
#                 "target_peer_id": req_peer_id
#             }
#             target_host, target_port = next((p[1], p[2]) for p in CLUSTER_MEMBERS if p[0] == req_peer_id)
#             send_message(target_host, target_port, ok_msg)
#             print(f"[{CLUSTER_ID}] Enviou OK para {req_peer_id} para requisição de {client_id}")
#         else:
#             print(f"[{CLUSTER_ID}] Não enviou OK para {req_peer_id}. Meu timestamp ({my_current_request[0] if my_current_request else 'N/A'}) vs deles ({client_timestamp}).")


# def process_ok_response(responding_peer_id, target_peer_id):
#     """Processa uma resposta OK recebida de outro membro do cluster."""
#     global ok_responses_received, my_current_request

#     if target_peer_id != CLUSTER_ID:
#         return # Este OK não é para mim

#     with lock:
#         if my_current_request is not None:
#             # Aumenta o contador de OKs apenas para a MINHA requisição atual
#             # Se você tiver múltiplas requisições pendentes, precisará de um dicionário
#             # para rastrear os OKs por requisição. Para este protocolo simples, assumimos uma por vez.
#             ok_responses_received += 1
#             print(f"[{CLUSTER_ID}] Recebeu OK de {responding_peer_id}. Total de OKs: {ok_responses_received}")

#             # Se todos os OKs foram recebidos, entrar na seção crítica
#             if ok_responses_received >= len(CLUSTER_MEMBERS): # Todos os 5 OKs
#                 print(f"[{CLUSTER_ID}] *** Entrando na SEÇÃO CRÍTICA ***")
#                 execute_critical_section()
#         else:
#             print(f"[{CLUSTER_ID}] Recebeu OK para uma requisição que não é a minha atual.")

# def execute_critical_section():
#     """Lógica da seção crítica e liberação."""
#     global my_current_request, ok_responses_received, pending_requests

#     if my_current_request is None:
#         return # Não há requisição para processar

#     client_id = my_current_request[1]
#     client_addr = client_sockets_for_reply.get(client_id)

#     if client_addr:
#         # Simula a escrita no Recurso R
#         sleep_time = random.uniform(0.2, 1.0)
#         print(f"[{CLUSTER_ID}] Escrevendo no Recurso R por {sleep_time:.2f}s...")
#         time.sleep(sleep_time)
#         print(f"[{CLUSTER_ID}] Escrita no Recurso R concluída.")

#         # Envia COMMITTED para o cliente original
#         try:
#             with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#                 s.connect(client_addr)
#                 s.sendall(b"COMMITTED")
#                 print(f"[{CLUSTER_ID}] Enviou COMMITTED para cliente {client_id} em {client_addr}")
#         except Exception as e:
#             print(f"[{CLUSTER_ID}] Erro ao enviar COMMITTED para {client_id}: {e}")
#         finally:
#             with lock:
#                 if client_id in client_sockets_for_reply:
#                     del client_sockets_for_reply[client_id]

#     # Libera a seção crítica e prepara para o próximo
#     with lock:
#         # Remove a requisição que acabou de ser processada da fila de pendentes
#         pending_requests = [req for req in pending_requests
#                             if not (req[0] == my_current_request[0] and req[1] == my_current_request[1] and req[2] == my_current_request[2])]

#         # Reinicia o estado para a próxima requisição
#         my_current_request = None
#         ok_responses_received = 0
#         print(f"[{CLUSTER_ID}] Seção crítica liberada. Estado resetado. Fila atual: {pending_requests}")

        
# # def release_critical_section():
# #     """Libera a seção crítica e prepara o peer para a próxima requisição."""
# #     global my_current_request, ok_responses_received, pending_requests

# #     with lock:
# #         # Remove a requisição que acabou de ser processada da fila de pendentes
# #         if my_current_request:
# #             # Encontra e remove a requisição correspondente na lista de pendentes
# #             # Baseado no timestamp e client_id, pois o req_peer_id pode não ser este peer
# #             pending_requests = [req for req in pending_requests
# #                                 if not (req[0] == my_current_request[0] and req[1] == my_current_request[1])]

# #             print(f"[{CLUSTER_ID}] Requisição {my_current_request} removida. Fila atual: {pending_requests}")

# #         my_current_request = None
# #         ok_responses_received = 0
# #         print(f"[{CLUSTER_ID}] Seção crítica liberada. Estado resetado.")


# def handle_connection(conn, addr):
#     try:
#         data = conn.recv(4096).decode('utf-8') # Aumentado o buffer para mensagens JSON
#         # Tenta desserializar como JSON
#         try:
#             message = json.loads(data)
#             if message["type"] == "REQUEST_CLUSTER":
#                 handle_cluster_request(message)
#             elif message["type"] == "OK":
#                 process_ok_response(message["responding_peer_id"], message["target_peer_id"])
#             else:
#                 print(f"[{CLUSTER_ID}] Mensagem de peer desconhecida: {data}")
#         except json.JSONDecodeError:
#             # Se não for JSON, trata como uma requisição de cliente (formato antigo)
#             print(f"[{CLUSTER_ID}] Recebeu mensagem não JSON, tratando como cliente: {data}")
#             handle_client_request(conn, addr, data)

#     except Exception as e:
#         print(f"[{CLUSTER_ID}] Erro ao lidar com conexão de {addr}: {e}")
#     finally:
#         # conn.close() # Fechado em handle_client_request, e para peers, a conexão pode ser de curta duração
#         pass

# def start_peer_server():
#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#         s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Permite reusar o endereço imediatamente
#         s.bind((HOST, PORT))
#         s.listen()
#         print(f"[{CLUSTER_ID}] Servidor escutando em {HOST}:{PORT}")

#         while True:
#             conn, addr = s.accept()
#             print(f"[{CLUSTER_ID}] Conexão aceita de {addr}")
#             thread = threading.Thread(target=handle_connection, args=(conn, addr))
#             thread.daemon = True # Permite que a thread termine com o programa principal
#             thread.start()

# # --- Função Principal ---

# if __name__ == "__main__":
#     print(f"[{CLUSTER_ID}] Meu ID: {CLUSTER_ID}, Meu Endereço: {HOST}:{PORT}")
#     print(f"[{CLUSTER_ID}] Membros do Cluster Conhecidos: {CLUSTER_MEMBERS}")

#     # Start the server in a separate thread to allow other operations if needed
#     server_thread = threading.Thread(target=start_peer_server)
#     server_thread.daemon = True # Make it a daemon thread
#     server_thread.start()

#     # Keep the main thread alive (or add other logic here)
#     # This loop keeps the main thread alive so daemon threads can run
#     try:
#         while True:
#             time.sleep(1)
#     except KeyboardInterrupt:
#         print(f"[{CLUSTER_ID}] Peer encerrado.")



































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