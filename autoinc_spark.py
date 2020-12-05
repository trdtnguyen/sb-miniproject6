from pyspark.sql import *

"""extract (key, value) from the input string.
Input: The string is a line in CSV file with format
    ID,type,vin_number,make,model, year, report_date, description
    For example:
    1,I,VXIO456XLBB630221,Nissan,Altima,2003,2002-05-08,Initial sales from TechMotors
    Output: dictionary {k:value}
    key: vin number
value: (type, make, year)
"""
def extract_vin_key_value(line: str):
   str_vals = line.split(',')
   type_val = str_vals[1]
   vin_num_val = str_vals[2]
   make_val = str_vals[3]
   year_val = str_vals[5]
   value = (type_val, make_val, year_val)
   return (vin_num_val, value)

# Create spark context and read the data file
spark = SparkSession \
            .builder \
            .appName("Automobile Postsale Report") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()
sc = spark.sparkContext

#sc = SparkContext("local", "Automobile Postsale Report")
raw_rdd = sc.textFile("data.csv")

# map (key,value) with key is vin_number and value is (make, year, incident_type)
vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))

# perform group aggregation to populate make and year to all the records
enhance_make = vin_kv.groupByKey().flatMap(lambda kv: kv[1]).filter(lambda x: len(x[1]) > 0 and len(x[2]) > 0)

# Combine make, year to make-year
make_kv = enhance_make.map(lambda x: x[1] + '-' + x[2])

#Map each make-year to 1 and count by key
make_kv_count = make_kv.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)
print(*make_kv_count.collect(), sep = '\n')
