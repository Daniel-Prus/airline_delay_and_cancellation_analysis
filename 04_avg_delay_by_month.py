from mrjob.job import MRJob


class MRMonthlyDelay(MRJob):

    def mapper(self, _, line):
        (fl_date, op_carrier, op_carrier_fl_num, origin, dest, crs_dep_time, dep_time, dep_delay, taxi_out, wheels_off,
         wheels_on, taxi_in, crs_arr_time, arr_time, arr_delay, cancelled, cancellation_code, diverted,
         crs_elapsed_time, actual_elapsed_time, air_time, distance, carrier_delay, weather_delay, nas_delay,
         security_delay, late_aircraft_delay, unnamed) = line.split(",")

        if dep_delay == "":
            dep_delay = 0
        if arr_delay == "":
            arr_delay = 0

        dep_delay = float(dep_delay)
        arr_delay = float(arr_delay)
        month = fl_date[5:7]
        month = int(month)

        yield f'{month:02d}', (dep_delay, arr_delay)

    def reducer(self, key, values):
        counter = 0
        dep_delay_sum = 0
        arr_delay_sum = 0
        for value in values:
            counter += 1
            dep_delay_sum += value[0]
            arr_delay_sum += value[1]

        yield int(key), (dep_delay_sum / counter, arr_delay_sum / counter)[0:2]


if __name__ == '__main__':
    MRMonthlyDelay.run()
