# coding=utf-8
"""
    Proiect MapReduce ALPD
    Functii utile pentru proiect
"""
import multiprocessing
from os import listdir
from os.path import isfile, join
from collections import OrderedDict
from global_vars import PUNCTUATION_TRANS


def read_directory_content(_path):
    """
    Preia toate fisierele dintr-un director si le stocheaza intr-o lista pe care o returneaza
    :param _path: calea catre director
    :type _path: str
    :return: o lista cu toate fisierele din director
    :rtype: list
    """
    if len(_path) == 0:
        raise Exception('[!] Calea catre director este nula')
    else:
        _path = r'{}'.format(_path)
        input_files = [r'{}\{}'.format(_path, _file) for _file in listdir(_path) if isfile(join(_path, _file))]
    return input_files


def create_output_file(_filename, _path, output_count):
    """
    Creaza un fisier de iesire in care sunt mapate secvente de forma (cuvant:nr_aparitii)
    :param _filename: numele fisierului de iesire
    :param _path: calea target
    :param output_count: maparea cuvintelor
    :type output_count: dict
    :type _filename str
    """

    with open('{}\\out_files\\{}'.format(_path, _filename), 'wb+') as _output_file:
        _current_file = _output_file.name

        lines = ''
        for key, value in output_count.iteritems():
            lines += '{}:{}\r\n'.format(key, value)

        _output_file.write(lines)

    return _current_file


def file_to_words(_filename):
    """
    Citeste un fisier si returneaza un dictionar de forma < docIDx,{term1 : count1,term2 : count2,...,termn :countn} >
    :param _filename: numele fisierului care va fi citit
    :type _filename: str
    :return: un dictionar de forma < docIDx,{term1 : count1,term2 : count2,...,termn :countn} >
    :rtype: dict
    """
    output_count = OrderedDict()

    print '{} se ocupa de fisierul {} ...'.format(multiprocessing.current_process().name, _filename)

    with open(_filename, 'rt') as _file_handler:
        for line in _file_handler:
            line = line.translate(PUNCTUATION_TRANS)
            words = line.split()
            for word in words:
                word = word.lower()
                if word.isalpha() and len(word) >= 6:
                    if word in output_count:
                        output_count[word] += 1
                    else:
                        output_count[word] = 1

    # preia numele fisierului din path
    _file_out = _filename.split('\\')[-1]
    relative_path = _filename.split('\\')[:-1]
    _path = '\\'.join(relative_path)

    # creaza fisierul intermediar de iesire (numele)
    _current_out_file = create_output_file(_file_out, _path, output_count)

    return _current_out_file


def count_words(item):
    """
    Numara de cate ori apare un cuvant dintr-un item din datele partitionate
    :param item: un element din data partitionata
    :return: o tupla cu numarul de aparitii
    """
    cuvant, numar_aparitii = item
    return cuvant, sum(numar_aparitii)


def reduce_phase(_filename):
    """
    Citeste din fisierele intermediare si creaza secevnte de forma < termk,{docIDk1 :countk1,docIDk2 : countk2,...,docIDkm : countkm} >
    :param _filename: numele fisierului intermediar
    :type _filename: str
    :return: un dictionar de forma < termk,{docIDk1 :countk1,docIDk2 : countk2,...,docIDkm : countkm} >
    """

    print '{} se ocupa de fisierul de iesire {} ...'.format(multiprocessing.current_process().name, _filename)
    collector_object = OrderedDict()

    with open(_filename, 'rb') as _file_handler:

        _file = _file_handler.name.split('\\')[-1]

        for line in _file_handler:
            line = line.split(':')
            cuvant = line[0].strip()
            nr_aparitii = int(line[1].strip())

            if cuvant not in collector_object:
                collector_object[cuvant] = OrderedDict()
            collector_object[cuvant][_file] = nr_aparitii

    return collector_object
