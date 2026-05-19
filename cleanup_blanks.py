import os
from pathlib import Path

def remove_blank_files(start_path="."):
    count = 0
    # Search through all files in the directory and subdirectories
    for path in Path(start_path).rglob('*'):
        if path.is_file():
            # Check if file size is 0 bytes
            if path.stat().st_size == 0:
                print(f"🗑️ Deleting blank file: {path}")
                path.unlink()
                count += 1
                
    print(f"\n✅ Cleanup complete. Deleted {count} blank files.")

if __name__ == "__main__":
    remove_blank_files()
