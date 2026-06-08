import json
import os
import subprocess
import time


HDFS_BASE = os.environ.get("AML_HDFS_BASE", "hdfs://10.84.129.52:9000/trab/g12")
SPARK_MASTER = os.environ.get("AML_SPARK_MASTER", "spark://10.84.128.47:7077")

INCREMENTAL_PATH = os.environ.get("AML_INCREMENTAL_PATH", f"{HDFS_BASE}/training/incremental")
COUNTER_PATH = os.environ.get("AML_COUNTER_PATH", f"{HDFS_BASE}/training/manifests/_counter.json")
ACTIVE_MODEL_PATH = os.environ.get("AML_ACTIVE_MODEL_PATH", f"{HDFS_BASE}/model/active_model.json")
MODEL_PATH = os.environ.get("AML_MODEL_PATH", f"{HDFS_BASE}/model/rf_aml_pipeline")
MODEL_VERSIONS_DIR = os.environ.get("AML_MODEL_VERSIONS_DIR", f"{HDFS_BASE}/model/versions")

RETRAIN_THRESHOLD = int(os.environ.get("AML_RETRAIN_THRESHOLD", "5000"))
POLL_INTERVAL = int(os.environ.get("AML_POLL_INTERVAL", "10"))
TRAINER_SCRIPT = os.environ.get("AML_TRAINER_SCRIPT", "aml_trainer.py")


def hdfs_cat_json(path):
    """Read a small JSON file from HDFS using the HDFS CLI."""
    proc = subprocess.run(
        ["hdfs", "dfs", "-cat", path],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0 or not proc.stdout.strip():
        return None
    try:
        return json.loads(proc.stdout)
    except ValueError:
        return None


def counter_rows():
    meta = hdfs_cat_json(COUNTER_PATH)
    if not meta:
        return 0
    return int(meta.get("total_rows", 0))


def active_model_meta():
    return hdfs_cat_json(ACTIVE_MODEL_PATH)


def run_trainer(incremental_rows_at_train, include_incremental=True):
    env = os.environ.copy()
    env.update({
        "AML_SPARK_MASTER": SPARK_MASTER,
        "AML_HDFS_BASE": HDFS_BASE,
        "AML_INCLUDE_INCREMENTAL": "1" if include_incremental else "0",
        "AML_INCREMENTAL_PATH": INCREMENTAL_PATH,
        "AML_INCREMENTAL_ROWS_AT_TRAIN": str(incremental_rows_at_train),
        "AML_MODEL_PATH": MODEL_PATH,
        "AML_MODEL_VERSIONS_DIR": MODEL_VERSIONS_DIR,
        "AML_ACTIVE_MODEL_PATH": ACTIVE_MODEL_PATH,
    })
    print(f"[auto-trainer] A executar: spark-submit {TRAINER_SCRIPT}")
    subprocess.run(["spark-submit", TRAINER_SCRIPT], check=True, env=env)


def maybe_train():
    active = active_model_meta()
    incr_now = counter_rows()

    if active is None:
        print(f"[auto-trainer] Sem modelo ativo. Treino inicial. "
              f"Incrementais atuais: {incr_now:,}")
        run_trainer(incr_now, include_incremental=incr_now > 0)
        print("[auto-trainer] Modelo inicial publicado pelo trainer.")
        return

    trained_at_incr = int(active.get("incremental_rows_at_train", 0))
    new_rows = incr_now - trained_at_incr
    print(f"[auto-trainer] Incrementais: {incr_now:,}  "
          f"(no último treino: {trained_at_incr:,})  novas: {new_rows:,}  "
          f"threshold: {RETRAIN_THRESHOLD:,}")

    if new_rows >= RETRAIN_THRESHOLD:
        print("[auto-trainer] Threshold atingido — a chamar aml_trainer.py...")
        run_trainer(incr_now)
        print("[auto-trainer] Novo modelo publicado pelo trainer.")
    else:
        print("[auto-trainer] Sem dados suficientes para retreinar.")


def main():
    print(f"[auto-trainer] A monitorizar contador: {COUNTER_PATH}")
    print(f"[auto-trainer] threshold={RETRAIN_THRESHOLD:,}  poll={POLL_INTERVAL}s")
    try:
        while True:
            try:
                maybe_train()
            except subprocess.CalledProcessError as exc:
                print(f"[auto-trainer] ERRO no trainer: {exc}")
            except Exception as exc:  # keep loop alive
                print(f"[auto-trainer] ERRO no ciclo: {exc}")
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print("[auto-trainer] Interrompido pelo utilizador.")


if __name__ == "__main__":
    main()
