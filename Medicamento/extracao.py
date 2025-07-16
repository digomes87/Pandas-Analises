import os
import requests
import pandas as pd
import re
import chardet
import time
from io import StringIO
from tqdm import tqdm

CKAN_URL = "https://opendatasus.saude.gov.br/api/3/action"
DATASET_ID = "doses-aplicadas-pelo-programa-de-nacional-de-imunizacoes-pni-2021"
TEMP_DIR = "temp"

os.makedirs(TEMP_DIR, exist_ok=True)

def extrair_mes(nome):
    match = re.search(r'(janeiro|fevereiro|março|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)', nome.lower())
    return match.group(1) if match else "desconhecido"

def download_csv_com_cache(url, mes_nome):
    temp_path = os.path.join(TEMP_DIR, f"{mes_nome}.csv.part")

    resume_header = {}
    downloaded = 0

    if os.path.exists(temp_path):
        downloaded = os.path.getsize(temp_path)
        resume_header = {"Range": f"bytes={downloaded}-"}

    r = requests.get(url, stream=True, headers=resume_header)
    r.raise_for_status()

    total = int(r.headers.get('Content-Length', 0)) + downloaded
    mode = 'ab' if downloaded > 0 else 'wb'

    with open(temp_path, mode) as f, tqdm(
        total=total,
        initial=downloaded,
        unit='B',
        unit_scale=True,
        desc=f"📥 {mes_nome}",
        ncols=80
    ) as pbar:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
                pbar.update(len(chunk))

    # Leitura segura com pandas
    with open(temp_path, 'rb') as f:
        content = f.read()

    encoding = chardet.detect(content)["encoding"]
    print(f"→ Encoding detectado: {encoding}")
    return pd.read_csv(StringIO(content.decode(encoding)), sep=';', low_memory=False)

def list_resources():
    r = requests.get(f"{CKAN_URL}/package_show", params={"id": DATASET_ID})
    r.raise_for_status()
    return r.json()["result"]["resources"]

def main():
    resources = list_resources()
    dfs = []
    print(f"🔎 Total de recursos encontrados: {len(resources)}")
    for res in resources:
        fmt = res.get("format", "").lower()
        name = res.get("name", "")
        url = res.get("url", "")
        if fmt == "csv" and "2021" in name:
            mes_nome = extrair_mes(name)
            print(f"\n🔽 Baixando: {name}")
            try:
                start = time.time()
                df = download_csv_com_cache(url, mes_nome)
                tempo = round(time.time() - start, 2)
                print(f"✅ {name} processado em {tempo} segundos")
                df["mes"] = mes_nome
                dfs.append(df)
            except Exception as e:
                print(f"❌ Erro ao processar {name}: {e}")

    if dfs:
        print("\n💾 Consolidando todos os dados...")
        full = pd.concat(dfs, ignore_index=True)
        full.to_csv("PNI_2021_doses.csv", index=False)
        print("✅ Arquivo final gerado: PNI_2021_doses.csv")
    else:
        print("⚠️ Nenhum arquivo CSV foi processado com sucesso.")

if __name__ == "__main__":
    main()


    # Apenas para o push
