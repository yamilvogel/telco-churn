import argparse
from pathlib import Path
import pandas as pd

def normalize_yes_no(series):
    # Normaliza variantes (Yes/No, Y/N, True/False, 1/0) a {Yes, No}
    mapping_true = {"yes", "y", "true", "1", "si", "sí"}
    mapping_false = {"no", "n", "false", "0"}
    out = []
    for v in series.astype(str):
        v_low = v.strip().lower()
        if v_low in mapping_true:
            out.append("Yes")
        elif v_low in mapping_false:
            out.append("No")
        else:
            out.append(pd.NA)
    return pd.Series(out, index=series.index, dtype="string")

def run(input_csv: str, output_parquet: str):
    # 1) Leer
    df = pd.read_csv(input_csv)

    # 2) Renombres típicos (ajustables según tu dataset real)
    #    Si los nombres ya están bien, no pasa nada.
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # 3) Forzar tipos básicos y arreglos comunes
    #    - customer_id si existe → string
    for key in ["customer_id", "customerid", "id"]:
        if key in df.columns:
            df[key] = df[key].astype("string")

    #    - churn si existe → {0,1} (acepta Yes/No/1/0/True/False)
    if "churn" in df.columns:
        s = df["churn"].astype(str).str.strip().str.lower()
        df["churn"] = s.isin(["yes", "y", "true", "1", "si", "sí"]).astype("int8")

    #    - columnas de tipo Yes/No frecuentes (ajusta la lista a tu dataset)
    yn_candidates = [c for c in df.columns if any(tok in c for tok in ["_service", "_protection", "paperless", "partner", "dependents", "phone", "internet", "techsupport", "streaming", "device", "online"])]
    for c in yn_candidates:
        try:
            df[c] = normalize_yes_no(df[c]).astype("string")
        except Exception:
            pass

    #    - cargos numéricos que a veces vienen como texto (ej. total_charges)
    for c in ["monthly_charges", "total_charges", "tenure"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # 4) Guardar
    out_path = Path(output_parquet)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    print(f"[OK] Procesado → {out_path}  | Filas: {len(df)}  Columnas: {df.shape[1]}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="input_csv", required=True)
    parser.add_argument("--out", dest="output_parquet", required=True)
    args = parser.parse_args()
    run(args.input_csv, args.output_parquet)