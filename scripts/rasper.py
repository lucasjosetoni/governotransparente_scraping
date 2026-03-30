import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

def extrair_todos_os_campos():
    url = "https://governotransparente.com.br/transparencia/03769490/consulta/consolidada/empenho"
    
    params = {
        "inicio": "28/02/2023",
        "fim": "29/03/2023",
        "ano": "3",
        "clean": "false",
        "limit": "-1"
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://governotransparente.com.br/",
    }

    try:
        print("Iniciando captura de todos os campos via HTML...")
        response = requests.get(url, params=params, headers=headers, timeout=25)
        response.encoding = response.apparent_encoding 
        
        soup = BeautifulSoup(response.text, 'html.parser')
        tabela = soup.find('table', {'id': 'data-table'}) or soup.find('table')
        
        if not tabela:
            return "Erro: Tabela não encontrada."

        # Pega as linhas do corpo da tabela
        corpo = tabela.find('tbody')
        rows = corpo.find_all('tr') if corpo else tabela.find_all('tr')[1:] # fallback se não tiver tbody
        
        lista_final = []

        for row in rows:
            cols = row.find_all('td')
            if len(cols) < 5: continue
            
            # --- PROCESSAMENTO COLUNA 0 (Empenho, Data, Fornecedor) ---
            texto_c0 = cols[0].get_text(separator=" ", strip=True)
            match_id = re.search(r"Empenho:\s*(\d+)", texto_c0)
            match_dt = re.search(r"Data:\s*(\d{2}/\d{2}/\d{4})", texto_c0)
            
            id_val = match_id.group(1) if match_id else "N/A"
            dt_val = match_dt.group(1) if match_dt else "N/A"
            
            # Fornecedor: removemos os rótulos conhecidos para sobrar o nome
            fornecedor_val = texto_c0.replace(f"Empenho: {id_val}", "").replace(f"Data: {dt_val}", "").strip()
            fornecedor_val = fornecedor_val.strip('-').strip()

            # --- PROCESSAMENTO COLUNA 1 (Órgão e Histórico) ---
            # O órgão costuma estar dentro de <strong>, o histórico é o resto
            strong_tag = cols[1].find('strong')
            orgao_val = strong_tag.get_text(strip=True) if strong_tag else "N/A"
            
            # Pega o texto total e remove o órgão para isolar o histórico
            texto_completo_c1 = cols[1].get_text(separator=" ", strip=True)
            historico_val = texto_completo_c1.replace(orgao_val, "").strip()

            # --- PROCESSAMENTO VALORES FINANCEIROS ---
            def to_num(txt):
                try:
                    return float(txt.replace('R$', '').replace('.', '').replace(',', '.').strip())
                except:
                    return 0.0

            # --- GARANTINDO TODOS OS CAMPOS SOLICITADOS ---
            lista_final.append({
                "empenhado": to_num(cols[2].text),
                "idEmpenho": id_val,
                "liquidado": to_num(cols[3].text),
                "gasto": to_num(cols[4].text), # 'gasto' associado ao valor Pago
                "empenho": id_val,
                "orgao": orgao_val,
                "fornecedor": fornecedor_val,
                "data": dt_val,
                "historico": historico_val,
                "dataDesc": dt_val,
                "empenhadoDesc": cols[2].get_text(strip=True),
                "liquidadoDesc": cols[3].get_text(strip=True),
                "gastoDesc": cols[4].get_text(strip=True)
            })

        if not lista_final:
            return "Processamento concluído, mas nenhum dado foi extraído."

        # Gerar DataFrame e CSV
        df = pd.DataFrame(lista_final)
        df.to_csv("dados_completos_estudo.csv", index=False, encoding='utf-8-sig')
        
        return f"Sucesso! {len(df)} registros salvos com todas as colunas pedidas."

    except Exception as e:
        return f"Falha: {str(e)}"

if __name__ == "__main__":
    print(extrair_todos_os_campos())