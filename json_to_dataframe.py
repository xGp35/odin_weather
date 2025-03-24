import pandas as pd
import json

# Read the JSON file
with open('template_odin_response.json', 'r') as file:
    data = json.load(file)

# Extract only the 'results' part
records = data['results']

# Convert to DataFrame and flatten nested structures
df = pd.json_normalize(records, sep='_')

# Function to safely parse JSON string
def parse_json_string(json_str):
    if pd.isna(json_str):
        return json_str
    try:
        if isinstance(json_str, str):
            return json.loads(json_str)
        return json_str
    except:
        return json_str

# Parse escaped JSON in specific columns
json_columns = ['incident', 'outagearea', 'names', 'incident_location']

# Create new columns for each nested JSON field
for col in json_columns:
    if col in df.columns:
        # Parse the JSON string in the column
        parsed_series = df[col].apply(parse_json_string)
        
        # Only process if the column contains dictionaries
        if parsed_series.apply(lambda x: isinstance(x, dict)).any():
            # Convert the parsed dictionaries to a DataFrame
            nested_df = pd.json_normalize(parsed_series.dropna(), sep='_')
            
            # Add new columns with prefix
            for nested_col in nested_df.columns:
                new_col_name = f"{col}_{nested_col}"
                df[new_col_name] = nested_df[nested_col]
            
            # Optionally, drop the original column
            df = df.drop(columns=[col])

# Save to CSV
df.to_csv('output_parsed.csv', index=False)

# Display information about the DataFrame
print("\nColumns in the final DataFrame:")
print(df.columns.tolist())
print("\nFirst few rows of the DataFrame:")
print(df.head())
