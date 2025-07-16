import time
import pandas as pd
from extractor import listar_recursos, filtrar_csvs_2021
from downloader import baixar_com_resumo
from utils import extrair_mes

def main():
    print("🔍 Buscando recursos...")
    recursos = listar_recursos()
    print(f"📦 Recursos encontrados: {len(recursos)}")
    arquivos_csv = filtrar_csvs_2021(recursos)

    dfs = []

    for r in arquivos_csv:
        nome = r.get("name", "")
        url = r.get("url", "")
        mes = extrair_mes(nome)
        print(f"\n🔽 Iniciando: {nome}")
        try:
            inicio = time.time()
            df = baixar_com_resumo(url, mes)
            df["mes"] = mes
            dfs.append(df)
            duracao = round(time.time() - inicio, 2)
            print(f"✅ {nome} processado em {duracao}s")
        except Exception as e:
            print(f"❌ Falha em {nome}: {e}")

    if dfs:
        consolidado = pd.concat(dfs, ignore_index=True)
        consolidado.to_csv("PNI_2021_doses.csv", index=False)
        print("✅ CSV final salvo como PNI_2021_doses.csv")
    else:
        print("⚠️ Nenhum dado processado.")

if __name__ == "__main__":
    main()

