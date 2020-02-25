import pandas as pd

result = pd.DataFrame.from_dict([{'D': 'D', 'Date': '2018-02-26', 'Time': '00:00:00', 'Tag ID': '3D6.00184CB8C3', 'Antenna': 'U1'}])
result = result.to_dict(orient="records")
result = pd.DataFrame.from_dict(result)

print(result.sample(n=1))
#
#
# def round_to_nearest_time_in_list(time, time_list):
#
#     min_timedelta = time_diff(time, time_list[0])
#     rounded_time = time_list[0]
#
#     for x in time_list:
#         delta = time_diff(time, x)
#         if delta <= min_timedelta:
#             min_timedelta = delta
#             rounded_time = x
#     return rounded_time
#
#
# def time_diff(x, y):
#     if x > y:
#         return x - y
#     else:
#         return y - x
#
#
# time_list = [pd.to_timedelta('03:00:00'), pd.to_timedelta('09:00:00'), pd.to_timedelta('15:00:00'), pd.to_timedelta('21:00:00')]
#
# tests = [
#             (pd.to_timedelta('06:00:00'), pd.to_timedelta('09:00:00')),
#             (pd.to_timedelta('04:00:00'), pd.to_timedelta('03:00:00')),
#             (pd.to_timedelta('13:00:00'), pd.to_timedelta('15:00:00')),
#             (pd.to_timedelta('17:00:00'), pd.to_timedelta('15:00:00')),
#             (pd.to_timedelta('23:00:00'), pd.to_timedelta('21:00:00')),
#             (pd.to_timedelta('00:00:00'), pd.to_timedelta('03:00:00')),
#             (pd.to_timedelta('21:00:00'), pd.to_timedelta('21:00:00')),
#             (pd.to_timedelta('01:00:00'), pd.to_timedelta('03:00:00')),
#             (pd.to_timedelta('10:00:00'), pd.to_timedelta('09:00:00')),
#             (pd.to_timedelta('05:00:00'), pd.to_timedelta('03:00:00'))]
#
# for x in tests:
#     print(x[0], x[1])
#     result = round_to_nearest_time_in_list(x[0], time_list)
#     print(result)
#     print(True if result == x[1] else False)


