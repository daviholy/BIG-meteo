from statistics import mean
from datetime import date
from mrjob.job import MRJob, MRStep
import csv
import json

class MeanOutTemp(MRJob):
    def mapper_day(self,csv_path,csv_uri):
        with open(csv_path) as file:
            reader = csv.DictReader(file)
            for row in reader:
                 yield row["datetime"].split(' ')[0], row["outtemp"]

    def reducer_mean(self, date, out_temps):
        yield date, mean([float(temp) for temp in out_temps])

    def mapper_max(self,date, mean_temp):
        yield date[:7], mean_temp

    def reducer_max(self, month, mean_temps):
        tmp = [mean_temp for mean_temp in mean_temps]
        yield month, {"max":max(tmp),"min":min(tmp)}

    def mapper_only_max(self, month, max_min):
        yield month, max_min["max"]


    def steps(self):
        return [
            MRStep(mapper_raw=self.mapper_day, reducer=self.reducer_mean),
            MRStep(mapper=self.mapper_max,reducer=self.reducer_max),
            MRStep(mapper=self.mapper_only_max)
        ]

if __name__ == '__main__':
    MeanOutTemp.run()
