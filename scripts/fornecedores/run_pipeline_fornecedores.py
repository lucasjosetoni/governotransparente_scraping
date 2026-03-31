import os
import subprocess
import sys


ROOT = os.path.dirname(__file__)

STEPS = [
    "00_select_fornecedores_unicos.py",
    "01_normalizar_fornecedores_gemini.py",
    "02_consultar_cnpj_brasilapi.py",
    "03_persistir_fornecedores_enriquecidos.py",
]


def run_step(step):
    cmd = [sys.executable, os.path.join(ROOT, step)]
    print(f"Executando: {step}")
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"Falha na etapa {step} (exit={result.returncode})")


def main():
    for step in STEPS:
        run_step(step)
    print("Pipeline de fornecedores concluido com sucesso.")


if __name__ == "__main__":
    main()
