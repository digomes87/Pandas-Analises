import os
import chardet
import pandas as pd
from tqdm import tqdm
from io import StringIO
import requests
from config import TEMP_DIR

os.makedirs(TEMP_DIR, exist_ok=True)

def baixar_com_resumo(url: str, nome: str) -> pd.DataFrame:
    temp_path = os.path.join(TEMP_DIR, f"{nome}.csv.part")
    resume_header = {}
    downloaded = 0

    if os.path.exists(temp_path):
        downloaded = os.path.getsize(temp_path)
        resume_header = {"Range": f"bytes={downloaded}-"}

    r = requests.get(url, stream=True, headers=resume_header)
    r.raise_for_status()

    total = int(r.headers.get('Content-Length', 0)) + downloaded
    modo = 'ab' if downloaded > 0 else 'wb'

    with open(temp_path, modo) as f, tqdm(
        total=total,
        initial=downloaded,
        unit='B',
        unit_scale=True,
        desc=f"📥 {nome}",
        ncols=80
    ) as pbar:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
                pbar.update(len(chunk))

    with open(temp_path, 'rb') as f:
        content = f.read()

    encoding = chardet.detect(content)["encoding"]
    return pd.read_csv(StringIO(content.decode(encoding)), sep=';', low_memory=False)

