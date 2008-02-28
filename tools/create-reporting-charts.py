#!/usr/bin/python
#
# Copyright (C) 2008 Polytechnique.org
# Author: Vincent Zanotti (vincent.zanotti@polytechnique.org)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Generates charts for the Google Apps reporting data which are stored in the
GApps sql database, and are synchronized by gappsd.

Usage:
  create-reporting-charts.py \
    --config-file /path/to/config/file \
    --destination /path/to/produced/charts
It results in currently two files:
  apps-activity-monthly.png
  apps-activity-yearly.png

TODO(vzanotti): Add vertical bars to indicate weeks.
"""

# Sets up the python path for 'gappsd' and pygooglechart modules inclusion.
import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'pygooglechart'))

import collections
import datetime
import gappsd.config, gappsd.database
import optparse
import pygooglechart


class ChartCreator(object):
  """Creates chart for the Google Apps reporting data, using the Google Chart
  API. Example usage:
    chart = ChartCreator('gappsd config file name')
    chart.StoreChartsTo('/path/to/charts/')
  """

  def __init__(self, config_file):
    self._config = gappsd.config.Config(config_file)
    self._sql = gappsd.database.SQL(self._config)

  def GetDataIndexValues(self, sql_query, sql_args, interval):
    """Retrieves the data for the @p sql_query and @p sql_args, and returns them
    date-indexed for the last @p intervals days. Missing dates receives a None value.
    If the interval is longer than 2 months, aggregates the result by week.
    
    Actually returns the (daylist, data) tuple."""

    # Retrieves data from the database.
    sql_data = self._sql.Query(sql_query, sql_args)

    # Determines current mode.
    week_aggregation = (interval > 61)
    if week_aggregation:
      start_date = datetime.date.today() - datetime.timedelta(interval)
      start_date -= datetime.timedelta(start_date.weekday())
    else:
      start_date = datetime.date.today() - datetime.timedelta(interval)

    # Data-indexes the data (and aggregates the data where required).
    day_list = []
    result = {}
    
    if week_aggregation:
      # Groups the data by week.
      weekly_count = collections.defaultdict(int)
      for row in sql_data:
        week_start = row["date"] - datetime.timedelta(row["date"].weekday())
        if week_start not in result:
          result[week_start] = collections.defaultdict(int)

        weekly_count[week_start] += 1
        for key in row:
          if key != "date" and row[key]:
            result[week_start][key] += row[key]

      # Averages the weekly counts, and creates the final result.
      for week in result:
        for key in result[week]:
          result[week][key] = int(result[week][key] / float(weekly_count[week]))

      # Adds 'None' result for weeks without data.
      for i in range(0, interval + 6, 7):
        date = start_date + datetime.timedelta(i)
        day_list.append(date)
        if not date in result:
          result[date] = None
    else:
      # Adds a default None value for every day in the interval.
      for i in range(0, interval):
        date = start_date + datetime.timedelta(i)
        result[date] = None
        day_list.append(date)

      # Updates the result with the sql data.
      for row in sql_data:
        result[row["date"]] = row

    return (day_list, result)


  def GetNormalizedData(self, data, normalization_factor):
    """Returns the normalized data for the @p normalization factor. Correctly
    handles None values."""

    return \
      [int(round(d * normalization_factor)) if d != None else d for d in data]

  def AddDataSerieToChart(self, chart, data, day_list, normalization_factor,
                          lambda_function=None, fill_color=None):
    """Adds the data serie to the chart, using the fill_color when required.
    The data serie is computer from the @p data, using the @p day_list as X
    coordinates, and the @p lmbda to actually compute the values.
    The data serie is normalized by the @p normalization factor.
    """

    serie = [None if not data[date] else lambda_function(data[date]) \
             for date in day_list]
    index = chart.add_data(self.GetNormalizedData(serie, normalization_factor))

    if fill_color:
      chart.add_fill_range(fill_color, index, index - 1)
    return index

  def SetChartXAxisLabels(self, chart, day_list):
    """Sets the proper date X-Axis labels for the @p chart, using date based on
    the @p day_list. Tries to add important dates (first day of month/week)."""

    # Adds dates to appear as labels.
    labels = [day for day in day_list if day.weekday() == 0]
    if not day_list[0] in labels:
      labels.append(day_list[0])
    if not day_list[-1] in labels:
      labels.append(day_list[-1])
    labels.sort()

    # Removes overlapping labels.
    guard_interval = int(round(0.2 * (day_list[-1] - day_list[0]).days))
    prev_day = None
    for day in labels[:]:
      if prev_day and day < prev_day + datetime.timedelta(guard_interval):
        labels.remove(day)
      else:
        prev_day = day

    # Prepares label positions and updates the chart.
    normalization_factor = 100 / float((day_list[-1] - day_list[0]).days)
    positions = \
      [int((d - day_list[0]).days * normalization_factor) for d in labels]
    text_labels = [d.strftime('%Y-%m-%d') for d in labels]
    axis_index = chart.set_axis_labels(pygooglechart.Axis.BOTTOM, text_labels)
    chart.set_axis_positions(axis_index, positions)

  def SetChartYAxisLabels(self, chart, max_value):
    """Sets the appropriate Y-Axis labels, and returns the real maximum value.
    """

    # Finds a 'human-readable' maximum value for Y Axis.
    real_max_value = 1
    while max_value > 10:
      real_max_value *= 10
      max_value /= float(10)

    if max_value <= 2:
      real_max_value *= 2
    elif max_value <= 4:
      real_max_value *= 4
    elif max_value <= 6:
      real_max_value *= 6
    elif max_value <= 8:
      real_max_value *= 8
    else:
      real_max_value *= 10

    # Updates the chart.
    positions = [25, 50, 75, 100]
    axis_index = chart.set_axis_labels(
      pygooglechart.Axis.LEFT,
      [str(int(real_max_value * pos / 100.0)) for pos in positions])
    chart.set_axis_positions(axis_index, positions)

    return real_max_value


  def GetUserActivityChart(self, interval):
    """Generates the Chart object for the account activity, based on reporting
    data stored in the gapps_reporting table.
    The data are plotted for the last @p interval days."""

    # If the interval is longer than two months, the data will be aggrated for
    # every week.
    if interval > 61:
      start_date = datetime.date.today() - datetime.timedelta(interval)
      start_date -= datetime.timedelta(start_date.weekday())
      aggregation_interval = 7
    else:
      start_date = datetime.date.today() - datetime.timedelta(interval)
      aggregation_interval = 1
    (day_list, data) = self.GetDataIndexValues(
      "SELECT * " \
      "FROM gapps_reporting " \
      "WHERE date >= DATE_SUB(CURDATE(), INTERVAL %s DAY) " \
      "ORDER BY date",
      (interval,),
      interval)
    chart = pygooglechart.SimpleLineChart(500, 250, title='Accounts activity')

    # Chart axis and max value preparation.
    max_value = \
      max([data[date]["num_accounts"] for date in day_list if data[date]])
    real_max_value = self.SetChartYAxisLabels(chart, max_value)
    normalization_factor = \
      pygooglechart.SimpleData.max_value() / float(real_max_value)

    self.SetChartXAxisLabels(chart, day_list)

    # Data serie preparation. Data set are added in decreasing order in order
    # to get proper ordering in legend.
    line_index = self.AddDataSerieToChart(
      chart, data, day_list, normalization_factor,
      lambda row: row["num_accounts"])
    chart.set_line_style(line_index, 2)
      
    self.AddDataSerieToChart(
      chart, data, day_list, normalization_factor,
      lambda row: row["num_accounts"] - row["count_90_day_idle"],
      fill_color="ffebcc")
    self.AddDataSerieToChart(
      chart, data, day_list, normalization_factor,
      lambda row: row["num_accounts"] - \
                  row["count_90_day_idle"] - \
                  row["count_60_day_idle"],
      fill_color="ffd799")
    self.AddDataSerieToChart(
      chart, data, day_list, normalization_factor,
      lambda row: row["count_30_day_actives"], fill_color="ffc266")
    self.AddDataSerieToChart(
      chart, data, day_list, normalization_factor,
      lambda row: row["count_7_day_actives"], fill_color="ffae33")
    self.AddDataSerieToChart(
      chart, data, day_list, normalization_factor,
      lambda row: 0, fill_color="ff9900")
    
    chart.set_legend((
      '# accounts',
      '90 days actives',
      '60 days actives',
      '30 days actives',
      '7 days actives',
    ))
    chart.set_colours((
      '76a4fb',    # for data_accounts.
      'efc789',    # for data_90d_actives.
      'efb256',    # for data_60d_actives.
      'ef9e23',    # for data_30d_actives.
      'ef8900',    # for data_7d_actives.
      'ffffffff',  # for the base line (X-Axis).
    ))

    return chart

  def StoreChartsTo(self, destination):
    """Retrieves the two charts, and stores them to the destination directory"""

    chart_month = self.GetUserActivityChart(31)
    chart_month.download(os.path.join(destination, 'apps-activity-monthly.png'))

    chart_year = self.GetUserActivityChart(365)
    chart_year.download(os.path.join(destination, 'apps-activity-yearly.png'))


if __name__ == '__main__':
  parser = optparse.OptionParser()
  parser.add_option("-c", "--config-file", action="store", dest="config_file")
  parser.add_option("-d", "--destination", action="store", dest="destination")
  (options, args) = parser.parse_args()

  if options.config_file is None or options.destination is None:
    print "Error: options --config-file and --destination are mandatory."
    exit(1)

  chart_creator = ChartCreator(options.config_file)
  chart_creator.StoreChartsTo(options.destination)
