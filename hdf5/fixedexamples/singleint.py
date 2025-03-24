import h5py
import numpy as np

# Define file and dataset name
file_name = "singleint.h5"
dataset_name = "temperature"

# Create an HDF5 file and write a single int64 value
with h5py.File(file_name, "w") as hdf_file:
    # Create the dataset at root level with a single int64 value
    hdf_file.create_dataset(dataset_name, data=np.int64(30))

print(f"HDF5 file '{file_name}' created successfully.")

with h5py.File("singleint.h5", "r") as hdf_file:
    temp_value = hdf_file["temperature"][()]
    print("Temperature:", temp_value)  # Should print: Temperature: 30
