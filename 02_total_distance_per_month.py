from mrjob.job import MRJob


class MRTotalDistance(MRJob):

    def mapper(self, _, line):
        (fl_date, op_carrier, op_carrier_fl_num, origin, dest, crs_dep_time, dep_time, dep_delay, taxi_out, wheels_off,
         wheels_on, taxi_in, crs_arr_time, arr_time, arr_delay, cancelled, cancellation_code, diverted,
         crs_elapsed_time, actual_elapsed_time, air_time, distance, carrier_delay, weather_delay, nas_delay,
         security_delay, late_aircraft_delay, unnamed) = line.split(",")

        if distance == "":
            distance = 0

        month = fl_date[5:7]
        month = int(month)

        yield f'{month:02d}', (float(distance))

    def reducer(self, key, values):
        yield int(key), int(sum(values))


if __name__ == "__main__":
    MRTotalDistance.run()
