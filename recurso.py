import random
# No processo do Cluster Sync que detém a "seção crítica"
recurso_r_conteudo = "Conteúdo inicial"

def escrever_no_recurso_r(novo_conteudo):
    global recurso_r_conteudo
    print(f"Escrevendo no Recurso R: '{novo_conteudo}'")
    recurso_r_conteudo = novo_conteudo
    # Simula o trabalho da escrita
    import time
    time.sleep(random.uniform(0.2, 1.0))
    print(f"Escrita no Recurso R concluída. Novo conteúdo: '{recurso_r_conteudo}'")