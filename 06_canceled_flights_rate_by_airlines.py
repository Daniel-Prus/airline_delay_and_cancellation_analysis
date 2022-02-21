from mrjob.job import MRJob
from mrjob.step import MRStep


class MRCanceledFlights(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer
                   )
        ]

    def configure_args(self):
        super(MRCanceledFlights, self).configure_args()
        self.add_file_arg('--airlines', help='Path to the airlines.csv')

    def mapper(self, _, line):
        (fl_date, op_carrier, op_carrier_fl_num, origin, dest, crs_dep_time, dep_time, dep_delay, taxi_out, wheels_off,
         wheels_on, taxi_in, crs_arr_time, arr_time, arr_delay, cancelled, cancellation_code, diverted,
         crs_elapsed_time, actual_elapsed_time, air_time, distance, carrier_delay, weather_delay, nas_delay,
         security_delay, late_aircraft_delay, unnamed) = line.split(",")
        cancelled = float(cancelled)
        yield op_carrier, int(cancelled)

    def reducer_init(self):
        self.airline_names = {}
        with open('airlines.csv', 'r') as file:
            for line in file:
                code, name = line.split(',')
                name = name[0:-2]
                self.airline_names[code] = name

    def reducer(self, key, values):
        counter = 0
        cancelled_flights_number = 0
        for value in values:
            counter += 1
            cancelled_flights_number += value
        yield (key, self.airline_names[key]), cancelled_flights_number / counter


if __name__ == '__main__':
    MRCanceledFlights.run()
