import ast
import json

import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.trigger import AfterProcessingTime, AccumulationMode
from apache_beam.transforms.window import(
TimestampedValue,
Sessions,
Duration
)
from apache_beam.io.textio import ReadFromText

#ParDO class to parse data into dict format
class parseinfo(beam.DoFn):
    def process(self, element):
        #readinf from txt file reads the tuple as string, so use ast() to read the string literally which is tuple
        element = ast.literal_eval(element)
        #extract each element from the tuple
        userID = element[0]
        score = element[1]
        time = element[2]
        return [{'userID': userID,
                 'score': score,
                  'time': time}]

#creating key/value from element and timestpmaing from the unix timestamp that data has
class AddTimestampDoFn(beam.DoFn):
    def process(self, element):
        unix_timestamp = element["time"]
        element = (element["userID"], element["score"])

        yield TimestampedValue(element, unix_timestamp)

#function to get the timestamp of each element
class GetTimestamp(beam.DoFn):
  def process(self, element, timestamp=beam.DoFn.TimestampParam):
    #print("element = ", element, type(element))
    time= timestamp.to_utc_datetime()
    yield '{} - {} -{}'.format(time, element[0], element[1])


#according to the apache rules, three functions where acc stores the intermediate values
class AverageFn(beam.CombineFn):
  def create_accumulator(self):
    sum = 0.0
    count = 0
    accumulator = sum, count
    return accumulator

  def add_input(self, accumulator, input):
    sum, count = accumulator
    return sum + input, count + 1

  def merge_accumulators(self, accumulators):
    # accumulators = [(sum1, count1), (sum2, count2), (sum3, count3), ...]
    sums, counts = zip(*accumulators)
    # sums = [sum1, sum2, sum3, ...]
    # counts = [count1, count2, count3, ...]
    return sum(sums), sum(counts)

  def extract_output(self, accumulator):
    sum, count = accumulator
    if count == 0:
      return float('NaN')
    return sum / count


#main runner code
def main(argv=None, save_main_session=True):

    beam_options = PipelineOptions(
            project='loganalysis',
            job_name='job1')
    print("---------starting pipeline-------")

    while True:
    #infinite loop to continously run the pipleline, to understand better remove the loop and run once only
        with beam.Pipeline(options=beam_options) as p:
                #Reading input data from logs file
                step1 = p | ReadFromText("log2.csv", skip_header_lines=1)
                #passing the pcollection to parseinfo fucntion to convert the format
                step2 = step1 | beam.ParDo(parseinfo())
                #Adding timestamp and getting the new pollection with k,v pairs
                step3 = step2 | "AddTimestamp" >> beam.ParDo(AddTimestampDoFn())
                #function to get timestamp from unix to readbale format
                steptemp1 = step3 | 'Get timestamp' >> beam.ParDo(GetTimestamp())
                #Printing function in the pipeline
                print1 = steptemp1 | 'print timestamp' >> beam.Map(print)
                #Dividing the data into fixed window chunks
                step4 = step3 | beam.WindowInto(window.FixedWindows(10))
                #grouping by key and then combing by sum for each key in the given window
                step5 = step4 | beam.CombinePerKey(sum)
                #combing by avergae
                steptemp2 = step4 | "avg" >> beam.CombinePerKey(AverageFn())
                #printing the sum, same can be done for avg
                step6 = step5 | "printSUM" >> beam.Map(print)
                #step4 = step3 | beam.io.WriteToText('result')




if __name__ == '__main__':
  main()