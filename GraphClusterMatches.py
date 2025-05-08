#This code is the process to be run after InventorRevelioDataFilter.py
#The users output of that file, has a many to many mapping of invnetor_ids to a linkedin users
#this code consolidates that data, and uses the union of all inventor_ids to distinguish people from one another

import pandas as pd
import networkx as nx
import builtins
import argparse

parser = argparse.ArgumentParser(description="is this a test or full suite, and are you using single or multi crosswalk")
parser.add_argument("--test", action="store_true", help="Set this flag to True for testing")
parser.add_argument("--multi", action="store_false", help="Set this flag to False for single mode")

args = parser.parse_args()

# Override the built-in print function, so that it will print where it ends up, even if it crashes later. 
def print(*args, **kwargs):
    # Set flush=True by default
    kwargs.setdefault("flush", True)
    builtins.print(*args, **kwargs)

print("import worked, reading data")

if args.multi:
    users = pd.read_csv("/uufs/chpc.utah.edu/common/home/fengj-group2/patents/inventor_to_user_multi.csv")
else:
    users = pd.read_csv("/uufs/chpc.utah.edu/common/home/fengj-group2/patents/inventor_to_user_single.csv")

# Step 1: Identify subID columns
subid_columns = [col for col in users.columns if "inventor_id" in col]

print("below are the collumns that are defined to disambiguate individuals")
print(subid_columns)

if args.test:
    print('doing a test case, just gonna pull a little bit')
    max_value_info = None
    max_count = 0

    for col in subid_columns:
        value_counts = users[col].value_counts()
        most_common_value = value_counts.idxmax()
        count = value_counts.max()
        if count > max_count:
            max_count = count
            max_value_info = (col, most_common_value)

    most_common_col, most_common_value = max_value_info
    users = users[users[most_common_col] == most_common_value]

print('read data, now creating graph')

# Step 2: Build a graph for row connections
graph = nx.Graph()
for idx in users.index:
    #create a node for each row
    graph.add_node(idx)
for col in subid_columns:    
    #create a dictionary to represent connections
    value_to_rows = {}
    for idx, value in users[col].items():
        if pd.notna(value):  # Ignore NaN values
            if value not in value_to_rows:
                #if not initialized, create a new array
                value_to_rows[value] = []
            #once the value has been seen, connect the row to it
            value_to_rows[value].append(idx)
    
    #iterate through the set of connections
    for rows in value_to_rows.values():
        for i in range(len(rows)):
            for j in range(i + 1, len(rows)):
                #add a connection for each i,j pair in the connections
                graph.add_edge(rows[i], rows[j])

print('graph created, now finding connected components')

#compute a list of connected components, every connected component is a unique person. 
connected_components = list(nx.connected_components(graph))

print("Finished distinguishing people, there were " + str(len(connected_components)) + " unique people found after graph search")

threshold = 1
for comp in connected_components:
    if len(comp) > threshold:
        print(comp)

# Step 3: Assign `person_id`
person_id_mapping = {}
for person_id, rows in enumerate(connected_components):
    for row in rows:
        person_id_mapping[row] = f"person{person_id}"
users['person_id'] = users.index.map(person_id_mapping)

# Step 4: Check for an existing one-to-one mapping
one_to_one_column = None
for col in subid_columns:
    if col == "person_id":
        continue
    if users.groupby(col)['person_id'].nunique().eq(1).all():
        one_to_one_column = col
        break

if one_to_one_column:
    print(f"One-to-one mapping found in column: {one_to_one_column}")
else:
    print("No one-to-one mapping exists. Generated `person_id`.")

# Step 5: Create and write the crosswalks
# Crosswalk for `disamb_inventor_id_20240630`
person_inventor_crosswalk = users.melt(
    id_vars=['person_id'],
    value_vars=subid_columns,
    value_name='disamb_inventor_id_20240930'
).dropna(subset=['disamb_inventor_id_20240930'])
person_inventor_crosswalk = person_inventor_crosswalk[['person_id', 'disamb_inventor_id_20240930']]

if not args.test:
    if args.multi:
        person_inventor_crosswalk.to_csv('/uufs/chpc.utah.edu/common/home/fengj-group2/patents/personInventorCrosswalkMulti.csv', index=False)
    else:
        person_inventor_crosswalk.to_csv('/uufs/chpc.utah.edu/common/home/fengj-group2/patents/personInventorCrosswalkSingle.csv', index=False)

print("wrote inventor crosswalk")

# Crosswalk for `revelio_user_id`
person_user_crosswalk = users[['person_id', 'revelio_user_id']].drop_duplicates()

if not args.test:
    if args.multi:
        person_user_crosswalk.to_csv('/uufs/chpc.utah.edu/common/home/fengj-group2/DISCERN-Revelio/personUserCrosswalkMulti.csv', index=False)
    else:
        person_user_crosswalk.to_csv('/uufs/chpc.utah.edu/common/home/fengj-group2/DISCERN-Revelio/personUserCrosswalkSingle.csv', index=False)

print("wrote linkiedin crosswalk")

