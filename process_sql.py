import pandas as pd
import os

# === CONFIGURATION ===
# Use workspace-relative paths (avoid creating folders at root '/')
# Path to your large SQL file (relative to repository root)
sql_file = "emotion_dataset_jira.sql"

# Folder to save CSV files (will be created if not exists)
output_dir = "jira_csv_output"
os.makedirs(output_dir, exist_ok=True)

# List of tables you want to process
# (You can update this list based on your SQL dump)
tables = ["projects", "issues", "comments", "authors"]

# Function to convert INSERT statements buffer to CSV
def write_table_to_csv(table_name, buffer):
    data = []
    for line in buffer:
        if line.upper().startswith("INSERT INTO"):
            # Extract values part
            values_part = line.split("VALUES")[1].strip().rstrip(';')
            # Split multiple rows separated by '),('
            for row in values_part.split("),("):
                row_clean = row.strip("()")
                row_values = [v.strip().strip("'" ) for v in row_clean.split(",")]
                data.append(row_values)
    if data:
        df = pd.DataFrame(data)
        csv_file = os.path.join(output_dir, f"{table_name}.csv")
        df.to_csv(csv_file, index=False)
        print(f"✅ Written {len(data)} rows to {csv_file}")

# === MAIN PROCESS ===
current_table = None
buffer = []

print(f"Looking for SQL file: {sql_file}")

if not os.path.exists(sql_file):
    print(f"❌ Error: The file '{sql_file}' was not found.")
    print("Please make sure the SQL file is in the root of the project directory.")
else:
    with open(sql_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            # Detect table creation
            if line.upper().startswith("CREATE TABLE"):
                # Save previous table
                if current_table and buffer:
                    write_table_to_csv(current_table, buffer)
                    buffer = []

                # Get table name
                parts = line.split()
                if len(parts) >= 3:
                    current_table = parts[2].strip('"')
                    if current_table not in tables:  # skip unlisted tables
                        current_table = None
                print(f"Processing table: {current_table}")

            # Collect INSERT statements
            elif line.upper().startswith("INSERT INTO") and current_table:
                buffer.append(line)

    # Write last table
    if current_table and buffer:
        write_table_to_csv(current_table, buffer)

    print("✅ All tables processed! CSV files are ready in:", output_dir)
