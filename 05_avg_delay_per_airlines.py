from mrjob.job import MRJob
from mrjob.step import MRStep


# run with --airlines flag
# python .\05_avg_delay_per_airlines.py .\rawdata_2018.csv --airlines .\airlines.csv > .\output\05_avg_delay_per_airlines.csv

class MRAirlinesDelay(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        (fl_date, op_carrier, op_carrier_fl_num, origin, dest, crs_dep_time, dep_time, dep_delay, taxi_out, wheels_off,
         wheels_on, taxi_in, crs_arr_time, arr_time, arr_delay, cancelled, cancellation_code, diverted,
         crs_elapsed_time, actual_elapsed_time, air_time, distance, carrier_delay, weather_delay, nas_delay,
         security_delay, late_aircraft_delay, unnamed) = line.split(",")

        if dep_delay == '':
            dep_delay = 0
        if arr_delay == '':
            arr_delay = 0

        dep_delay = float(dep_delay)
        arr_delay = float(arr_delay)

        yield op_carrier, (dep_delay, arr_delay)

    def configure_args(self):
        super(MRAirlinesDelay, self).configure_args()
        self.add_file_arg('--airlines', help='Path to the airlines.csv')

    def reducer_init(self):
        self.airline_names = {}
        with open('airlines.csv', 'r') as file:
            for line in file:
                code, name = line.split(',')
                name = name[0:-2]
                self.airline_names[code] = name

    def reducer(self, key, values):
        dep_delay_sum = 0
        arr_delay_sum = 0
        counter = 0

        for value in values:
            dep_delay_sum += value[0]
            arr_delay_sum += value[1]
            counter += 1

        yield (key, self.airline_names[key]), (dep_delay_sum / counter, arr_delay_sum / counter)


if __name__ == '__main__':
    MRAirlinesDelay.run()
