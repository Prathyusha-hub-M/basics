import os, shutil
from pathlib import Path

folder_path = Path(r"C:\Users\prath\fileorganizerdemo")
files = os.listdir(folder_path)
print(files)
#Mapping files to their extensions
file_types = {}
for file in files:
    file_path = folder_path / file
    ext = file_path.suffix.lower()
    file_types[file_path] = ext
    # file_path.append(Path(file))
    # file_type.append(file_path.suffix.lower())
    # print(file_type)
# for file in files:
#       print(file_types[file])
# print(files)

# print(os.path.abspath(folder_path))

#Mapping folder_names and types
folder_names = {"Image Files":[".png", ".jpg",".jpeg"],
                "CSV Files":[".csv"],
                "Text Files":[".txt"]}

#Create folders if not exist
for name in folder_names:
       (folder_path/name).mkdir(exist_ok=True)

#Check if file exists in the corresponding folder if not move it 
for file_path, ext in file_types.items():
        for folder, extensions in folder_names.items():
              if ext in extensions:
                    destination = folder_path / folder / file_path.name
                    shutil.move(str(file_path), str(destination))
                    print(f"moved {file_path} to {folder}") 
