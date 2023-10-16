import configparser

config=configparser.RawConfigParser()
config.read("c:\\Users\\megala\\documents\\view_comparison\\config.ini')

class ReadConfig():

  #Get the value from config file based on key
  def getData(value1,value2):
      Data=config.get(value1, value2)
      return Data
