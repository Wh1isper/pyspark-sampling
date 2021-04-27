import json
import os

if __name__ == '__main__':
    new_path = 'hdfs://localhost:9000/dataset/10w_x_10.csv'
    g = os.walk(r"./requestbody")
    files = []
    for path, dir_list, file_list in g:
        for file_name in file_list:
            files.append(os.path.join(path, file_name))

    for file in files:
        try:
            with open(file) as f:
                config = json.load(f)
            if config.get('path'):
                config['path'] = new_path
            with open(file, 'w') as f:
                json.dump(config, f)
        except:
            continue
