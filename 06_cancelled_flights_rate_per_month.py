from mrjob.job import MRJob
from mrjob.step import MRStep


class MRCanceledFlights(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer
                   )
        ]

    def mapper(self, _, line):
        (fl_date, op_carrier, op_carrier_fl_num, origin, dest, crs_dep_time, dep_time, dep_delay, taxi_out, wheels_off,
         wheels_on, taxi_in, crs_arr_time, arr_time, arr_delay, cancelled, cancellation_code, diverted,
         crs_elapsed_time, actual_elapsed_time, air_time, distance, carrier_delay, weather_delay, nas_delay,
         security_delay, late_aircraft_delay, unnamed) = line.split(",")

        cancelled = float(cancelled)
        month = fl_date[5:7]
        month = int(month)

        yield f'{month:02d}', int(cancelled)

    def reducer(self, key, values):
        counter = 0
        cancelled_flights = 0
        for value in values:
            cancelled_flights += value
            counter += 1
        yield int(key), cancelled_flights / counter


if __name__ == '__main__':
    MRCanceledFlights.run()
