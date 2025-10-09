# Install dependencies as needed:
# pip install kagglehub[pandas-datasets]
import kagglehub
from kagglehub import KaggleDatasetAdapter

# Set the path to the file you'd like to load
file_path = "itineraries.csv"

def load_dataset() -> "pd.DataFrame":
    attempts = (
        {},
        {"pandas_kwargs": {"encoding": "latin-1"}},
        {
            "pandas_kwargs": {
                "encoding": "latin-1",
                "engine": "python",
                "sep": None,
                "on_bad_lines": "skip",
            }
        },
    )
    last_err = None
    for kwargs in attempts:
        try:
            return kagglehub.dataset_load(
                KaggleDatasetAdapter.PANDAS,
                "dilwong/flightprices",
                file_path,
                **kwargs,
            )
        except ValueError as err:
            last_err = err
    raise last_err


df = load_dataset()

# save full dataset and a random sample of 120k rows
file_path = "../data/raw/flightprices_raw.csv"
df.to_csv(file_path, index=False)

sample_file_path = "../data/raw/flightprices_raw_small_random_sampled.csv"
df_sample = df.sample(n=min(120000, len(df)), random_state=42)
df_sample.to_csv(sample_file_path, index=False)
print(f"Sample saved to: {sample_file_path}")
print("Sample size:", len(df_sample))