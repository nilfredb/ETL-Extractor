# pipeline.py
import os
import sys
import subprocess


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def run_step(script_name: str) -> None:
    script_path = os.path.join(BASE_DIR, script_name)

    if not os.path.exists(script_path):
        print(f"[PIPELINE] ERROR: No se encontró el script: {script_path}")
        sys.exit(1)

    print(f"[PIPELINE] Ejecutando {script_name} ...")

    # Llama: python script_name
    result = subprocess.run(
        [sys.executable, script_path],
        cwd=BASE_DIR
    )

    if result.returncode != 0:
        print(f"[PIPELINE] ERROR: {script_name} terminó con código {result.returncode}")
        sys.exit(result.returncode)

    print(f"[PIPELINE] {script_name} finalizado correctamente.\n")


def main():
    print("======================================")
    print("   PIPELINE DW Opiniones (SQLite → DW)")
    print("======================================\n")

    # 1) Sincronizar dimensiones en el DW
    run_step("sync_dimensions_dw.py")

    # 2) Ejecutar el ETL principal (staging, dims locales, facts, carga al DW)
    run_step("main.py")

    print("\n[PIPELINE] Todo el pipeline terminó OK ✅")


if __name__ == "__main__":
    main()
