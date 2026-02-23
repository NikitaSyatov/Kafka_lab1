try:
    import kagglehub
except ModuleNotFoundError:
    print("Module kagglehub not found. Please use command for install: pip install kagglehub")
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