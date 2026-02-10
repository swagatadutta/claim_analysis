import os
import shutil

def read_sql_file(sql_file_path):
    with open(sql_file_path, "r", encoding="utf-8") as f:
        sql_query = f.read().strip()
    return sql_query

def save_spark_csv_to_output_csv(input_dir, output_dir):
    for f in os.listdir(f'{input_dir}/'):
        folder_path = f'{input_dir}/{f}/'
        os.makedirs(f'{output_dir}/', exist_ok=True)
        
        for c in os.listdir(folder_path):
            if c.endswith('.csv'):  # Only CSV files
                src = f'{folder_path}/{c}'
                dst = f'{output_dir}/{f}'
                shutil.copy2(src, dst)
                print(f"✅ Copied {f}/{c}")

def clear_directory(path):
    # Walk bottom‑up so we delete children before parents
    for root, dirs, files in os.walk(path, topdown=False):
        # Delete all files
        for name in files:
            file_path = os.path.join(root, name)
            os.remove(file_path)
        # Delete all subdirectories (they are empty now)
        for name in dirs:
            dir_path = os.path.join(root, name)
            os.rmdir(dir_path)