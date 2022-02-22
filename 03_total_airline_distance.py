from mrjob.job import MRJob
from mrjob.step import MRStep

# run with --airlines flag
# python .\03_total_airline_distance.py .\rawdata_2018.csv --airlines .\airlines.csv

class MRAirlineDistance(MRJob):

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

        yield op_carrier, float(distance)

    def configure_args(self):
        super(MRAirlineDistance, self).configure_args()
        self.add_file_arg('--airlines', help='Path to the airlines.csv')

    def reducer_init(self):
        self.airline_names = {}
        with open('airlines.csv', 'r') as file:
            for line in file:
                code, name = line.split(',')
                name = name[0:-2]
                self.airline_names[code] = name

    def reducer(self, airline, distances):
        yield (self.airline_names[airline]), int(sum(distances))


if __name__ == "__main__":
    MRAirlineDistance.run()
