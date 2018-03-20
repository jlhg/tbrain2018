#!/usr/bin/env python
import os
import re

header_path = './header.txt'
data_dir_path = './data'
output_path = './data.csv'

def snake_case(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def header():
    h = []
    with open(header_path, 'r') as fi:
        for line in fi:
            h.append(line.strip())
    return h

def main():
    features = {}
    headers = header()
    for h in headers:
        if h == 'FileId':
            continue
        d = os.path.join(data_dir_path, snake_case(h))
        for file in os.listdir(d):
            with open(os.path.join(d, file), 'r') as fi:
                for line in fi:
                    file_id, value = line.strip().split(",")
                    if file_id not in features:
                        features[file_id] = {}
                    features[file_id][h] = value
    with open(output_path, 'w') as fo:
        fo.write(','.join(headers))
        fo.write('\n')
        fo.flush()
        for file_id, data in features.items():
            row = [file_id]
            for h in headers:
                if h == 'FileId':
                    continue
                row.append(features[file_id][h])
            fo.write(','.join(row))
            fo.write('\n')
            fo.flush()


if __name__ == '__main__':
    main()
