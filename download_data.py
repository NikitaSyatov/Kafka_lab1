import kagglehub
import shutil
import os

# Скачиваем датасет и получаем путь
downloaded_path = kagglehub.dataset_download("jacksoncrow/stock-market-dataset")
print("Dataset downloaded to:", downloaded_path)

target_dir = "./data"
os.makedirs(target_dir, exist_ok=True)

for item in os.listdir(downloaded_path):
    src = os.path.join(downloaded_path, item)
    dst = os.path.join(target_dir, item)
    if os.path.isfile(src):
        shutil.copy2(src, dst)
    elif os.path.isdir(src):
        shutil.copytree(src, dst, dirs_exist_ok=True)

print(f"Dataset copy in directory: {target_dir}")