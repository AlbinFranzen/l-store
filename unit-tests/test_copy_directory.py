import os
def copy_directory(src, dst):
    """
    Recursively copies a directory without using shutil.
    """
    os.makedirs(dst, exist_ok=True)  # Ensure destination directory exists

    for root, dirs, files in os.walk(src):
        rel_path = os.path.relpath(root, src)  # Compute relative path
        target_dir = os.path.join(dst, rel_path)

        os.makedirs(target_dir, exist_ok=True)  # Ensure each subdirectory exists

        for file in files:
            src_file = os.path.join(root, file)
            dst_file = os.path.join(target_dir, file)

            # Copy file contents manually
            with open(src_file, 'rb') as f_src, open(dst_file, 'wb') as f_dst:
                f_dst.write(f_src.read())  # Read from src, write to dst

dst = "./archive_test/"
src = "./random_files/"

copy_directory(src,dst)
print("hi")
