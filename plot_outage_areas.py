import json
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from shapely.geometry import shape

def plot_outage_areas(json_file):
    # Read the JSON file
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    # Create a list to store GeoDataFrames
    gdfs = []
    
    # Process each result in the JSON
    for result in data['results']:
        if 'geom' in result:
            # Convert the geometry to a shapely object
            geometry = shape(result['geom']['geometry'])
            
            # Create a GeoDataFrame for this polygon
            gdf = gpd.GeoDataFrame(
                geometry=[geometry],
                data={'name': [result['name']], 'state': [result['state']]}
            )
            gdfs.append(gdf)
    
    # Combine all GeoDataFrames
    if gdfs:
        combined_gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
        
        # Create the plot
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Plot each polygon
        for idx, row in combined_gdf.iterrows():
            row.geometry.plot(ax=ax, alpha=0.5, label=f"{row['name']} - {row['state']}")
        
        # Add legend and labels
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        ax.set_title('Power Outage Areas')
        plt.tight_layout()
        
        # Save the plot
        plt.savefig('outage_areas.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        print("Plot saved as 'outage_areas.png'")
    else:
        print("No valid geometries found in the JSON file")

if __name__ == "__main__":
    plot_outage_areas('template_odin_response.json') 