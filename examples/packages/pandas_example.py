import np as np
import pandas as pd
from matplotlib import pyplot as plt
import hazelcast

"""Create Hazelcast client connection"""
client = hazelcast.HazelcastClient()

"""Get a IMap for storing patient's DataFrame"""
patients = client.get_map("patients").blocking()

"""
Store the heart rate and blood pressure data of fifty patients as DataFrame objects in Hazelcast cluster.
"""
for pid in range(0, 50):
    """
    Create a random DataFrame.
    """
    df = pd.DataFrame(
        data={
            "blood_pressure": np.random.randint(80, 120, size=(75,)),
            "heart_rate": np.random.randint(60, 100, size=(75,)),
        },
        index=pd.date_range("1/1/2023", periods=75, freq="H"),
    )
    """
    There are multiple serialization methods for DataFrame. Hazelcast uses pickle serialization as default
    for various Python objects. So, you can simply put your data directly using default pickle serialization.

    patients.put("1", df)

    Alternatively, you can convert your DataFrame to JSON, CSV or a dict using to_json(), to_csv() and to_dict() methods.
    Notice that these methods returns string representation of DataFrame in requested format.

    Convert to CSV: df = df.to_csv()
    Convert to JSON: df = df.to_json()
    Convert to dict: df = df.to_dict()

    If you use these representation, you need to re-create DataFrame object as follows since they are stored as 
    string in Hazelcast cluster. For example, if you are converting DataFrame to CSV, use following while reading: 

    df = pd.from_csv(StringIO(patients.get("patient-1")))
    """
    patients.put(f"{pid}", df.to_json())

pid = np.random.randint(0, 50)
df = pd.read_json(patients.get(f"{pid}"))

df.plot(use_index=True, y=["blood_pressure", "heart_rate"], figsize=(15, 5), kind="line")
plt.title(f"Blood Pressure and Heart Rate Plot of Patient-{pid}")
plt.show()
