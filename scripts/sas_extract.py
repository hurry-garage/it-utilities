'''
Created on June 26, 2019

@author: e43765
'''

import tqdm
import csv
import datetime
from dba_process import conf
from dba_process.conf import RUN_ARGS
from sas7bdat import SAS7BDAT as sas


class SAS_Extractor(object):
    '''
    classdocs
    '''

    def __init__(self, input_file, output_file):
        '''
        '''
        self.input_file = input_file
        self.output_file = output_file
     
    def extract_sas_csv(self):
        '''
        '''
        with sas(self.input_file) as sasFile:
            with open(self.output_file, 'w', newline='') as csvFile:
                csvwriter = csv.writer(csvFile)
                for row in sasFile:
                    csvwriter.writerow(row)
                    
    def extract_all(self):
        '''
        '''
        self.extract_sas_csv()