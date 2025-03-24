import h5py
import numpy as np

FILE_NAME = "testpy.h5"
DATASET_NAME = "Demand"
NUM_RECORDS = 1750
STRUCT_SIZE = 56
buffer_size = NUM_RECORDS * STRUCT_SIZE
buffer = bytearray(buffer_size)

# ✅ Define structured NumPy dtype (Mimics C++ Packed Struct)
structured_dtype = np.dtype([
    ("shipmentId", "<u8"),
    ("origCountry", "S2"),
    ("origSlic", "S5"),
    ("origSort", "S1"),
    ("destCountry", "S2"),
    ("destSlic", "S5"),
    ("destIbi", "S1"),
    ("destPostalCode", "S9"),
    ("shipper", "S10"),
    ("service", "S1"),
    ("packageType", "S1"),
    ("accessorials", "S1"),
    ("pieces", "<u2"),
    ("weight", "<u2"),
    ("cube", "<u4"),
    ("committedTnt", "S1"),
    ("committedDate", "S1"),
])

with h5py.File(FILE_NAME, "w") as hdf_file:
    # ✅ Create dataset with compound type using high-level interface
    dset = hdf_file.create_dataset(
        DATASET_NAME,
        shape=(NUM_RECORDS,),
        dtype=structured_dtype  # Use the NumPy dtype directly
    )
    
    # ✅ ADD ATTRIBUTE: "GIT root revision" with fixed-length string
    attribute_name = "GIT root revision"
    attribute_value = "Revision: , URL: "
    # Create a fixed-length string datatype matching the value length
    fixed_str_dtype = h5py.string_dtype(encoding='ascii', length=len(attribute_value))
    # Create the attribute with the fixed-length type
    dset.attrs.create(attribute_name, attribute_value, dtype=fixed_str_dtype)

    # ✅ Populate buffer
    for i in range(NUM_RECORDS):
        offset = i * STRUCT_SIZE

        buffer[offset : offset + 8] = (1000 + i).to_bytes(8, "little")
        buffer[offset + 8 : offset + 10] = b"US"
        buffer[offset + 10 : offset + 15] = b"12345"
        buffer[offset + 15 : offset + 16] = b"L"
        buffer[offset + 16 : offset + 18] = b"CA"
        buffer[offset + 18 : offset + 23] = b"67890"
        buffer[offset + 23 : offset + 24] = b"N"
        buffer[offset + 24 : offset + 33] = b"A1B2C3".ljust(9, b"\x00")  # Ensure exact 9 bytes
        buffer[offset + 33 : offset + 43] = b"FedEx".ljust(10, b"\x00")  # Ensure exact 10 bytes
        buffer[offset + 43 : offset + 44] = b"E"
        buffer[offset + 44 : offset + 45] = b"B"
        buffer[offset + 45 : offset + 46] = b"N"
        buffer[offset + 46 : offset + 48] = (2).to_bytes(2, "little")
        buffer[offset + 48 : offset + 50] = (50).to_bytes(2, "little")
        buffer[offset + 50 : offset + 54] = (1200).to_bytes(4, "little")
        buffer[offset + 54 : offset + 55] = b"Y"
        buffer[offset + 55 : offset + 56] = b"D"

    # ✅ Convert buffer to structured NumPy array
    print(f"First Record Raw Bytes (i=0): {buffer[:STRUCT_SIZE].hex()}")  # 🚀 Debug Dump
    numpy_data = np.frombuffer(buffer, dtype=structured_dtype)

    # ✅ Write data to dataset
    dset[()] = numpy_data

print(f"HDF5 file '{FILE_NAME}' created successfully with dynamically built compound type.")

# Verify attribute
with h5py.File(FILE_NAME, "r") as hdf_file:
    dataset = hdf_file[DATASET_NAME]
    print(f"Attribute '{attribute_name}': {dataset.attrs[attribute_name]}")