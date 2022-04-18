from fileinput import filename
import os
from sqlite3 import Timestamp 
import json
from io import StringIO

def transform_2_json(log_file, json_directory, file_size):
    with open(log_file,"r",encoding='UTF-8',errors='ignore') as f:
        file_index = 0
        json_file = json_directory + "/file_" + str(file_index) + ".json"
        out_file = open(json_file,"w")
        result = {'dataset':[]}
        size_count = 0

        lines = f.readlines()
        for line in lines:
            if size_count >= file_size:
                record = json.dumps(result)
                out_file.write(record)
                out_file.close()
                file_index += 1
                json_file = json_directory + "/file_" + str(file_index) + ".json"
                out_file = open(json_file,"w")
                result = {'dataset':[]}
                size_count = 0
            array_element = {}
            r = line.split(' ')
            r = r[1:len(r)-1]
            entry_string = ""
            for element in r:
                entry_string = entry_string+ " " + element
            array_element["timestamp"] = int(r[0])
            array_element["entry"] = entry_string
            result['dataset'].append(array_element)
            size_count += 1

        record = json.dumps(result)
        out_file.write(record)
        out_file.close()

def main():
    log_file = "./thunder_bird.log"
    json_directory = "./json_directory"
    file_size = 1000
    transform_2_json(log_file,json_directory,file_size)

if __name__ == "__main__":
    main()
