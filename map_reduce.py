# coding=utf-8
"""
    Map Reduce Class
"""
import multiprocessing
import collections
from utils import reduce_phase


class MapReduce(object):
    """
        Clasa pentru Map reduce
    """

    def __init__(self, _functie_mapare, _functie_reduce, _path, numar_workers=None):
        """
        C-tor de initializare
        :param _functie_mapare: Functie de mapare -> functie care primeste ca parametru un nume de fisier, si returneaza
                                un dictionar de forma < docIDx,{term1 : count1,term2 : count2,...,termn :countn} >
        :param _functie_reduce: Functie de reducere -> functie care primeste un element din maparea facuta de functia de mapare
        :param _path: calea catre folderul target
        :param numar_workers: numarul de workers care va fi creat in pool-ul de threaduri
        """
        self.functie_mapare = _functie_mapare
        self.functie_reducere = _functie_reduce
        self.numar_workers = numar_workers
        self.pool = multiprocessing.Pool(processes=numar_workers)
        self.reduce_phase_collector = collections.OrderedDict()
        self.folder_path = _path

    @staticmethod
    def partition_function(mapped_values):
        """
        Organizeaza valorile mapate dupa cheie
        :return:
        """

        partitioned_data = collections.defaultdict(list)
        for key, value in mapped_values:
            partitioned_data[key].append(value)

            del mapped_values[key]

        return partitioned_data.items()

    def write_final_result(self):
        """
        Creaza un fisier rezultatul final de forma < termk,{docIDk1 :countk1,docIDk2 : countk2,...,docIDkm : countkm} >
        :return:
        """
        print '--------------------------------------------------------------- Etapa IV - Rezultatul final'
        if len(self.reduce_phase_collector.keys()) > 0:
            with open('{}\\final_result\\final_result.txt'.format(self.folder_path), 'wb+') as _file_handler:
                for cuvant, list_files in self.reduce_phase_collector.iteritems():
                    line = '< {} : {}'.format(cuvant, '{')

                    for item in list_files:
                        for _file, nr_aparitii in item.iteritems():
                            line += '{} : {}, '.format(_file, nr_aparitii)

                    line += '{} >\r\n'.format('}')
                    _file_handler.write(line)
                    del self.reduce_phase_collector[cuvant]

    def map_reduce(self, input_files, _chunksize=1):
        """
        Proceseaza fiecare fisier cu ajutorul functiilor de reduce si mapare
        :param input_files: O lista cu numele tuturor fisierelor care trebuie procesate
        :param _chunksize: Numarul de fisiere ca il va procesa
        :return:
        """

        # faza de mapare
        print '--------------------------------------------------------------- A - II - a Etapa - Fisiere Intermediare'
        map_response = self.pool.map(self.functie_mapare, input_files, chunksize=_chunksize)

        print '--------------------------------------------------------------- A - III - a Etapa - Etapa de reducere'
        _my_collection = self.pool.map(reduce_phase, map_response, chunksize=_chunksize)
        for item in _my_collection:
            for key, value in item.iteritems():
                if key not in self.reduce_phase_collector:
                    self.reduce_phase_collector[key] = [value]
                else:
                    self.reduce_phase_collector[key].append(value)

                item.pop(key, value)
