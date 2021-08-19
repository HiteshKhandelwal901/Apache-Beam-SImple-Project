import argparse
import logging
import re
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

print("inside main.py")

class foramt_csv(beam.DoFn):
    def process(self, element):
        print("element = ", element)
        values = element.split(",")
        #print("values = ", values)
        Date, Open, High, Low, Close, Volume, Name = element.split(",")
        return [{
            'Date' : Date,
            'Open' : Open,
            'High' : High,
            'Low'  : Low,
            'Close': Close,
            'Volume': Volume,
            'Name' : Name}]


class Format_date(beam.DoFn):
    def process(self, element):
        date = element['Date']
        #print("date before fromating  = ", date, "type = " ,type(date))
        datetimeobject = datetime.strptime(date, '%Y-%m-%d')
        formated_date = datetimeobject.strftime('%d-%m-%Y')
        #print("after date formatting = ", formated_date, "type", type(formated_date))
        print("element before processing", element)
        element['Date'] = formated_date
        print("element after processing = ", element, "type= ", type(element))
        return [{
            'Date' : formated_date,
            'Open' : element['Open'],
            'High' : element['High'],
            'Low': element['Low'],
            'Close': element['Close'],
            'Volume': element['Volume'],
            'Name': element['Name']
        }]





def main(argv=None, save_main_session=True):
    print("running man code")
    print("args = ", argv)

    input_file = 'data/all_stocks_5yr.csv'
    output_path = 'formated_dates.txt'

    parser = argparse.ArgumentParser()
    parser.add_argument(
          '--input',
          dest='input',
          default='/data',
          help='Input file to process.')
    parser.add_argument(
          '--output',
          dest='output',
          # CHANGE 1/6: The Google Cloud Storage path is required
          # for outputting the results.
          default='/output/result.txt',
          help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    print("known_args = ", known_args)

    beam_options = PipelineOptions(
        project='S&P',
        job_name='job1')

    print("---------starting pipeline-------")

    with beam.Pipeline(options=beam_options) as p:
        print("starting step 1")
        print("input = ", known_args.input)
        step1 =  p     | beam.io.ReadFromText(known_args.input, skip_header_lines=1)
        print("done step1 with result = ", step1)
        step2 =  step1 | beam.ParDo(foramt_csv())
        print("done step2 with result = ", step2)
        step3 =  step2 | beam.ParDo(Format_date())
        print("done step3 with result = ", step3)
        step4 = step3  | beam.io.WriteToText(known_args.output)
        print(" done with last step ")


if __name__ == '__main__':
  main()


