import apache_beam as beam
GROCERY_LIST = [
    beam.Row(recipe='pie', fruit='strawberry', quantity=3, unit_price=1.50),
    beam.Row(recipe='pie', fruit='raspberry', quantity=1, unit_price=3.50),
    beam.Row(recipe='pie', fruit='blackberry', quantity=1, unit_price=4.00),
    beam.Row(recipe='pie', fruit='blueberry', quantity=1, unit_price=2.00),
    beam.Row(recipe='muffin', fruit='blueberry', quantity=2, unit_price=2.00),
    beam.Row(recipe='muffin', fruit='banana', quantity=3, unit_price=1.00),
]

with beam.Pipeline() as p:
  grouped = p | beam.Create(GROCERY_LIST) | beam.GroupBy() | beam.Map(print)