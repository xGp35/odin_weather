import pandas as pd
import folium
import ast
import json

# Read the CSV file
df = pd.read_csv('filtered_outage_data.csv')

# Convert the string representation of coordinates to a Python list
coordinates = ast.literal_eval(df['geom_geometry_coordinates'].iloc[0])

# Create a map centered at the centroid
m = folium.Map(
    location=[df['centroid_lat'].iloc[0], df['centroid_lon'].iloc[0]],
    zoom_start=10
)

# Extract all points from the multipolygon coordinates
points = []
# The coordinates structure is [[[[lon, lat], [lon, lat], ...]]]
for polygon in coordinates:  # Access the multipolygon
    for ring in polygon:     # Access each polygon
        for point in ring:   # Access each point in the ring
            points.append(point)

# Plot each point on the map
for point in points:
    folium.CircleMarker(
        location=[point[1], point[0]],  # Note: folium uses [lat, lon] format
        radius=3,
        color='red',
        fill=True,
        fill_color='red',
        fill_opacity=1
    ).add_to(m)

# Add a layer control
folium.LayerControl().add_to(m)

# Save the map to an HTML file
m.save('outage_area_map.html') 