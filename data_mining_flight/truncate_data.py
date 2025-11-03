import os
import argparse
import pandas as pd

try:
    import matplotlib.pyplot as plt
except ImportError:
    plt = None


def resolve_paths(user_csv: str | None) -> str:
    """Find the CSV to truncate.
    If user_csv provided, return its absolute path.
    Else search data/raw for first existing candidate name.
    """
    pkg_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(pkg_dir, ".."))  # ascend two levels
    raw_dir = os.path.join(project_root, "data", "raw")
    candidates = [
        "itineraries.csv"
    ]
    if user_csv:
        csv_path = os.path.abspath(user_csv)
        if not os.path.isfile(csv_path):
            raise FileNotFoundError(f"Provided --csv not found: {csv_path}")
        return csv_path
    for name in candidates:
        p = os.path.join(raw_dir, name)
        if os.path.isfile(p):
            return p
    raise FileNotFoundError(
        f"No candidate CSV found in {p}. Looked for: {', '.join(candidates)}. "
        "Provide one via --csv."
    )


def compute_daily_counts(csv_path: str, date_column: str, chunksize: int) -> pd.DataFrame:
    counts = {}
    for chunk in pd.read_csv(
        csv_path,
        usecols=[date_column],
        dtype={date_column: "string"},
        chunksize=chunksize,
        low_memory=True,
    ):
        vc = chunk[date_column].value_counts()
        for d, c in vc.items():
            counts[d] = counts.get(d, 0) + int(c)
    df = pd.DataFrame(sorted(counts.items()), columns=[date_column, "count"])
    return df


def get_middle_date(daily_counts: pd.DataFrame, date_column: str) -> str:
    ordered = daily_counts.sort_values(date_column)
    mid_idx = len(ordered) // 2
    return ordered.iloc[mid_idx][date_column]


def write_truncated_day(csv_path: str, output_path: str, date_column: str, target_date: str, chunksize: int):
    first = True
    for chunk in pd.read_csv(csv_path, chunksize=chunksize, low_memory=True):
        if date_column not in chunk.columns:
            raise ValueError(f"Column '{date_column}' not found in CSV.")
        filtered = chunk[chunk[date_column] == target_date]
        if filtered.empty:
            continue
        filtered.to_csv(output_path, mode="w" if first else "a", index=False, header=first)
        first = False


def plot_counts(daily_counts: pd.DataFrame, date_column: str, output_dir: str):
    if plt is None:
        return
    x = pd.to_datetime(daily_counts[date_column], errors="coerce")
    plt.figure(figsize=(14, 4))
    plt.plot(x, daily_counts["count"], linewidth=1)
    plt.title(f"Row distribution per {date_column}")
    plt.xlabel(date_column)
    plt.ylabel("row count")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"{date_column}_counts.png"), dpi=150)
    plt.close()


def main():
    parser = argparse.ArgumentParser(description="Truncate large flight prices CSV to its middle day.")
    parser.add_argument("--csv", help="Path to source CSV (optional; auto-detect if omitted).")
    parser.add_argument(
        "--date-column",
        default="flightDate",
        help="Date column to use (e.g. searchDate or flightDate).",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=500_000,
        help="Rows per chunk while streaming.",
    )
    parser.add_argument("--middle-date", help="Override auto-selected middle date.")
    parser.add_argument("--no-plot", action="store_true", help="Disable plotting daily distribution.")
    args = parser.parse_args()

    csv_path = resolve_paths(args.csv)

    pkg_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(pkg_dir, ".."))
    output_dir = os.path.join(project_root, "data", "turncated")
    os.makedirs(output_dir, exist_ok=True)

    print(f"Resolved input CSV: {csv_path}")
    print(f"Output directory:   {output_dir}")

    daily_counts = compute_daily_counts(csv_path, args.date_column, args.chunksize)
    daily_counts_path = os.path.join(output_dir, f"{args.date_column}_daily_counts.csv")
    daily_counts.to_csv(daily_counts_path, index=False)

    middle_date = args.middle_date or get_middle_date(daily_counts, args.date_column)

    truncated_path = os.path.join(output_dir, f"{args.date_column}_{middle_date}.csv")
    write_truncated_day(csv_path, truncated_path, args.date_column, middle_date, args.chunksize)

    if not args.no_plot:
        plot_counts(daily_counts, args.date_column, output_dir)

    print(f"Middle date:        {middle_date}")
    print(f"Daily counts saved: {daily_counts_path}")
    print(f"Truncated file:     {truncated_path}")
    if not args.no_plot and plt is not None:
        print(f"Plot saved:         {os.path.join(output_dir, f'{args.date_column}_counts.png')}")
    if plt is None and not args.no_plot:
        print("matplotlib not installed; skipping plot.")


if __name__ == "__main__":
    main()
