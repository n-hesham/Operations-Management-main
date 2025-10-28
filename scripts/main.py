from pathlib import Path
from cleaning import clean_files
from save_to_sql import append_to_sqlite

def main():
    base_path = Path(__file__).resolve().parent.parent / "files"
    output_path = base_path / "cleaned_files"

    output_path.mkdir(parents=True, exist_ok=True)

    print(f"Processing files in: {base_path}")
    print("\n=== Starting Cleaning ===")
    clean_files(base_path, output_path)

    cleaned_file = output_path / "shipping_companies.xlsx"

    print("\n=== Saving to SQL Database ===")
    append_to_sqlite(cleaned_file)

    print("\nAll Complete!")

if __name__ == "__main__":
    main()
