import os
import pandas as pd
import requests
import tarfile


def download_dataset(houshold_number, data_folder):
    repo_url = "https://mygit.th-deg.de/tcg/gelap/-/raw/master/hh-" + str(household_number) + ".tar.xz?ref_type=heads"  # replace with your repo URL
    r = requests.get(repo_url)
    with open(data_folder + "hh-" + str(household_number) + ".tar.xz", "wb") as f:
        f.write(r.content)
    tar = tarfile.open(data_folder + "hh-" + str(household_number) + ".tar.xz")
    tar.extractall(data_folder + "hh-" + str(household_number))
    tar.close()
    os.remove(data_folder + "hh-" + str(household_number) + ".tar.xz")
    
def create_temp_device_csvs(household_path, labels, start_time, end_time, temp_dir):
    smartmeter = pd.read_csv(os.path.join(household_path, "smartmeter.csv"))
    smartmeter = smartmeter.set_index("time")
    smartmeter.index = pd.to_datetime(smartmeter.index, unit='ms')
    smartmeter.index = smartmeter.index.floor('s')
    smartmeter = smartmeter.loc[start_time:end_time]
    smartmeter["power"] = smartmeter["power1"] + smartmeter["power2"] + smartmeter["power3"]
    smartmeter = smartmeter.rename(columns={"power": "smartmeter"})
    smartmeter = smartmeter.drop(columns=["power1", "power2", "power3"])
    smartmeter.to_csv(os.path.join(temp_dir, "smartmeter.csv"))
    smartmeter = smartmeter.drop(columns=["smartmeter"])

    for label in labels:
        device = pd.read_csv(os.path.join(household_path, label))
        device["time"] = pd.to_datetime(device["time_request"], unit='ms')
        device["time"] = device["time"].dt.floor('s')
        device = device.drop(columns=["time_request", "time_reply"])
        device = device.set_index("time")
        device = device.resample("1s").mean()

        chunk = pd.DataFrame(index=smartmeter.index)
        chunk = chunk.join(device)
        chunk = chunk.rename(columns={"power": labels[label]})
        chunk.to_csv(os.path.join(temp_dir, labels[label] + ".csv"))
        
def merge_csvs(temp_dir, data_folder, household_number):
    # loop over folder and merge all csv files into one csv file
    files = os.listdir(temp_dir)
    df = pd.read_csv(os.path.join(temp_dir, files[0]))
    # rename the Unamed column to "time"
    df = df.rename(columns={"Unnamed: 0": "time"})
    # set the timestamp as the index
    df = df.set_index("time")
    for file in files[1:]:
        device = pd.read_csv(os.path.join(temp_dir, file))
        filename = os.path.splitext(file)[0]
        # get the column named after the filename and convert it to a list
        power = device[filename].tolist()
        # add the list to the dataframe
        df[filename] = power

    df.to_csv(os.path.join(data_folder, "hh-" + str(household_number) + "merged.csv"))
   
    
household_number = 14
data_folder = "data//"
household_path = data_folder + "hh-" + str(household_number)
labels = {"label_001.csv": "thermomix",
          "label_002.csv": "toaster",
          "label_003.csv": "coffee_machine", 
          "label_004.csv": "electric_kettle",
          "label_005.csv": "microwave",
          "label_006.csv": "radio",
          "label_007.csv": "washing_machine",
          "label_008.csv": "vacuum_cleaner",
          "label_009.csv": "television",
          "label_010.csv": "charger"}
start_time = '2020-03-17 09:30:00'
end_time = '2020-04-21 06:30:00'

temp_dir = os.path.abspath(os.path.join(data_folder, "temp"))
if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)

download_dataset(household_number, data_folder)
create_temp_device_csvs(household_path, labels, start_time, end_time, temp_dir)
merge_csvs(temp_dir, data_folder, household_number) 