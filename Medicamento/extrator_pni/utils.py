import re

def extrair_mes(nome: str) -> str:
    match = re.search(
        r"(janeiro|fevereiro|mar[çc]o|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)",
        nome.lower()
    )
    return match.group(1) if match else "desconhecido"

