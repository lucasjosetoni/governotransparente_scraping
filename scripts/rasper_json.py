import requests
import json
import time
import os

def extrair_json_bruto(ano, inicio, fim, limit="-1"):
    """
    Apenas extrai os dados da API e salva o JSON original.
    Não realiza limpezas ou conversões de tipo.
    """
    base_url = "https://governotransparente.com.br/app/portal/api/v1/json/despesa/consolidada/empenho/03769490"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json",
        "Referer": "https://governotransparente.com.br/"
    }

    params = {
        "ano": str(ano),
        "limit": str(limit),
        "inicio": inicio,
        "fim": fim
    }

    try:
        print(f"📥 Extraindo: Ano {ano} [{inicio} a {fim}]")
        response = requests.get(base_url, params=params, headers=headers, timeout=40)
        response.raise_for_status()
        
        dados = response.json()
        
        if not dados:
            print(f"⚠️  Aviso: Nenhum dado retornado para o período {inicio}.")
            return None

        # Garante que a pasta 'empenhos' existe
        output_dir = "empenhos"
        os.makedirs(output_dir, exist_ok=True)
        # Nome do arquivo baseado nos parâmetros de busca
        filename = os.path.join(output_dir, f"raw_empenhos_{ano}_{inicio.replace('/', '-')}.json")
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(dados, f, ensure_ascii=False, indent=4)
            
        print(f"✅ Arquivo salvo: {filename} ({len(dados)} registros)")
        return filename

    except Exception as e:
        print(f"❌ Falha na extração: {e}")
        return None

if __name__ == "__main__":
    # Lista de períodos para extração em lote
    periodos = [
        {"ano": 3, "inicio": "01/01/2023", "fim": "31/12/2023"},
        {"ano": 1, "inicio": "01/01/2024", "fim": "31/12/2024"},
        {"ano": 2, "inicio": "01/01/2025", "fim": "31/12/2025"}
    ]

    for p in periodos:
        extrair_json_bruto(**p)
        time.sleep(2) # Delay de segurança