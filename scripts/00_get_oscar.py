# scripts/00_get_oscar.py

from datasets import load_dataset
import os

# Define the output directory and file
output_dir = "data/raw/oscar"
output_file = os.path.join(output_dir, "oscar_nepali_raw.txt")
os.makedirs(output_dir, exist_ok=True)

print("Loading OSCAR dataset for Nepali. This may take a very long time...")
# Use streaming=True to avoid downloading the whole dataset into RAM
dataset = load_dataset("oscar", "unshuffled_deduplicated_ne", split="train", streaming=True)

print("Dataset loaded. Writing text to file. This will also take a while...")
count = 0
with open(output_file, "w", encoding="utf-8") as f:
    for example in dataset:
        # Each 'example' is a dictionary, we need the 'text' key
        f.write(example['text'] + "\n")
        count += 1
        if count % 10000 == 0:
            print(f"Processed {count} documents...")

print(f"Finished. Wrote {count} documents to {output_file}")