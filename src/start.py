from extract import extract
from load import load

def __main__():
   print("STARTING")
   print("EXTRACTING")
   extractData = extract()
   
   print("TRANSFORMING")
   #transformData = transform(extractData) 

   print("LOADING")
   loadData = load(extractData)
   print("DONE")

if __name__== "__main__":
    __main__()