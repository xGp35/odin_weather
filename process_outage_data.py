import pandas as pd

# Read the CSV file
df = pd.read_csv('one_line.csv')

# Select only the columns needed for mapping
columns_to_keep = [
    'name',
    'state',
    'county',
    'centroid_lon',
    'centroid_lat',
    'geo_point_2d_lon',
    'geo_point_2d_lat',
    'geom_geometry_coordinates'
]

# Create a new DataFrame with only the selected columns
filtered_df = df[columns_to_keep]

# Save the filtered data to a new CSV file
filtered_df.to_csv('filtered_outage_data.csv', index=False)

print("Data processing complete. Filtered data saved to 'filtered_outage_data.csv'")
print(f"Original columns: {len(df.columns)}")
print(f"Filtered columns: {len(filtered_df.columns)}") 