import pandas as pd
import json

# Read the JSON file
with open('template_odin_response.json', 'r') as file:
    data = json.load(file)

# Extract only the 'results' part which contains the actual records
records = data['results']

# Convert to DataFrame and flatten nested structures
df = pd.json_normalize(records, sep='_')

# Save to CSV
df.to_csv('output.csv', index=False)

# Optional: Display the first few rows of the dataframe
print(df.head())
