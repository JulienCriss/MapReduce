# coding=utf-8
"""
    Main module
"""

import os
from map_reduce import MapReduce
import utils as util_functions

if __name__ == '__main__':
    folder_path = raw_input("Calea catre director: ")
    numar_procese = input("Numarul de procese: ")

    if not os.path.exists('{}\\out_files'.format(folder_path)):
        os.makedirs('{}\\out_files'.format(folder_path))

    if not os.path.exists('{}\\final_result'.format(folder_path)):
        os.makedirs('{}\\final_result'.format(folder_path))

    list_of_files = util_functions.read_directory_content(folder_path)

    map_reduce_object = MapReduce(util_functions.file_to_words, util_functions.count_words, folder_path, numar_procese)
    print '--------------------------------------------------------------- Prima Etapa - Etapa de Mapare'
    map_reduce_object.map_reduce(list_of_files)
    map_reduce_object.write_final_result()
