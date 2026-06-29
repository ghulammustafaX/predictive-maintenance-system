import pandas as pd
import os

def load_cmapss_data(file_name):
 
    file_path = os.path.join("data", "cmapss", file_name)
    
    # 26 columns define 
    columns = ['engine_id', 'cycle', 'setting1', 'setting2', 'setting3'] + [f'sensor{i}' for i in range(1, 22)]
    
    print(f"🔍 Reading file from: {file_path} ...")
    
    # file path cheecking
    if not os.path.exists(file_path):
        print(f"❌ Error: File nahi mili! Path check karo: {file_path}")
        return None
        
    # File reading
    df = pd.read_csv(file_path, sep=r'\s+', header=None, names=columns)
    
    print("✅ Data successfully loaded!\n")
    print(f"📊 Total Rows or Columns (Shape): {df.shape}")
    print("\n--- Shuru ki 5 Rows ---")
    print(df.head())
    
    return df

if __name__ == "__main__":
    # Tum kisi bhi file ka naam yahan pass kar sakte ho 
    # (e.g., 'train_FD001.txt', 'test_FD002.txt', ya 'RUL_FD001.txt')
    dataset = load_cmapss_data("train_FD001.txt")