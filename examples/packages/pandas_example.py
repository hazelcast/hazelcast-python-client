"""
You can store Pandas DataFrame objects in Hazelcast cluster and retrieve with low-latency.
There are multiple serialization methods for DataFrame objects.
Hazelcast uses pickle serialization as default for various Python objects including DataFrame.
So, you can simply put your data directly using default pickle serialization without any conversions.

patients.put(1, df)

Alternatively, you can convert your DataFrame to JSON, CSV or a dict using to_json(), to_csv() and to_dict() methods.
Note that JSON or CSV serializations returns string representation of DataFrame in requested format.

Convert to CSV: df = df.to_csv()
Convert to JSON: df = df.to_json()
Convert to dict: df = df.to_dict()

If you prefer to use these converted representations, you need to re-create DataFrame object since they are not stored
as DataFrame object in Hazelcast cluster. For CSV, JSON and dict conversions, use following methods while retrieving:

Create from CSV: df = pd.from_csv(StringIO(patients.get(1)))
Create from JSON: df = pd.read_json(patients.get(1))
Create from dict object: df = pd.DataFrame(patients.get(1))

In addition to methods above, you can write your own custom serializer for DatFrame objects. For more information about
Pyton client serialization methods, see https://hazelcast.readthedocs.io/en/stable/serialization.html#
"""

import hazelcast
from hazelcast.core import HazelcastJsonValue
from matplotlib import pyplot as plt
import np as np
import pandas as pd

# Create Hazelcast client
client = hazelcast.HazelcastClient()

# Get an IMap for storing patient's DataFrame objects
patients = client.get_map("patients").blocking()

# Store the blood pressure and heart rate data of fifty patients in a DataFrame and load it to Hazelcast cluster as JSON
for pid in range(0, 50):
    # Create DataFrame with random values
    df = pd.DataFrame(
        data={
            "blood_pressure": np.random.randint(80, 120, size=(75,)),
            "heart_rate": np.random.randint(60, 100, size=(75,)),
        },
        index=pd.date_range("2023-01-15", periods=75, freq="H"),
    )
    # Load DataFrame to Hazelcast cluster as HazelcastJsonValue
    patients.put(pid, HazelcastJsonValue(df.to_json()))

pid = np.random.randint(0, 50)
# Retrieve the data of a random patient
df = pd.read_json(patients.get(pid).to_string())

# Plot the data
df.plot(use_index=True, y=["blood_pressure", "heart_rate"], figsize=(15, 5), kind="line")
plt.title(f"Blood Pressure and Heart Rate Plot of Patient-{pid}")
plt.show()
