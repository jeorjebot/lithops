"""
Simple Lithops example using the map method and
the context manager. In this example the map()
method will launch one map function for each entry
in 'iterdata'. Finally it will print the results
for each invocation with pw.get_result()
"""
import lithops


def my_map_function(id, x):
    print("I'm activation number {}".format(id))
    return x + 7


if __name__ == "__main__":
    iterdata = [1, 2, 3, 4]
    with lithops.ibm_cf_executor() as pw:
        pw.map(my_map_function, iterdata)
        print(pw.get_result())
