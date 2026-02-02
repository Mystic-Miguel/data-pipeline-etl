import argparse, pandas as pd, sys
def main():
    p = argparse.ArgumentParser(description="CSV -> Parquet ETL")
    p.add_argument("--input", required=True, help="input CSV")
    p.add_argument("--output", default="out.parquet", help="output Parquet")
    args = p.parse_args()
    df = pd.read_csv(args.input)
    df = df.drop_duplicates()
    for c in df.select_dtypes(include="object").columns:
        df[c] = df[c].str.strip()
    df.to_parquet(args.output, index=False)
    print(f"Wrote {args.output} ({len(df)} rows)")
if __name__ == "__main__":
    sys.exit(main())
