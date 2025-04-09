import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon, Point
import json
from pathlib import Path

def get_nc_cities():
    """Create a dataset of North Carolina cities."""
    # List of major cities in North Carolina with their coordinates
    cities_data = {
        'name': [
            'Windsor', 'Ahoskie', 'Aulander', 'Powellsville', 'Colerain',
            'Roxobel', 'Kelford', 'Lewiston Woodville', 'Rich Square',
            'Jackson', 'Conway', 'Murfreesboro', 'Scotland Neck'
        ],
        'lat': [
            35.9982, 36.2843, 36.2293, 36.1726, 36.1971,
            36.2024, 36.1904, 36.1168, 36.2743,
            36.3896, 36.4279, 36.4413, 36.1293
        ],
        'lon': [
            -76.9494, -76.9847, -77.0247, -76.8924, -76.7527,
            -77.2358, -77.2144, -77.1783, -77.2833,
            -77.4219, -77.2147, -76.9280, -77.4219
        ],
        'population': [
            3630, 4800, 895, 259, 204,
            220, 251, 533, 958,
            513, 836, 2835, 2059
        ]
    }
    return pd.DataFrame(cities_data)

def create_polygon_from_coordinates(coordinates_str):
    """Create a polygon from the coordinates string in the CSV."""
    try:
        # Clean up the string - remove any whitespace
        coordinates_str = coordinates_str.strip()
        # Parse the coordinates string from the CSV
        coordinates = json.loads(coordinates_str)
        # Extract the polygon coordinates (format is [[[[lon, lat], ...]]]])
        polygon_coords = coordinates[0][0]
        return Polygon(polygon_coords)
    except Exception as e:
        print(f"Error creating polygon: {e}")
        return None

def main():
    try:
        # Read the outage data
        outage_df = pd.read_csv('filtered_outage_data.csv')
        
        # Get the polygon coordinates
        polygon_coords = outage_df['geom_geometry_coordinates'].iloc[0]
        polygon = create_polygon_from_coordinates(polygon_coords)
        
        if polygon is None:
            print("Failed to create polygon from coordinates")
            return
        
        print("Successfully created polygon from coordinates")
        
        # Get NC cities data
        cities_df = get_nc_cities()
        
        # Convert to GeoDataFrame
        geometry = [Point(xy) for xy in zip(cities_df['lon'], cities_df['lat'])]
        cities_gdf = gpd.GeoDataFrame(cities_df, geometry=geometry, crs="EPSG:4326")
        
        # Create a GeoDataFrame with the polygon
        polygon_gdf = gpd.GeoDataFrame(geometry=[polygon], crs="EPSG:4326")
        
        # Find cities within the polygon
        cities_in_polygon = cities_gdf[cities_gdf.geometry.within(polygon)]
        
        # Save to CSV
        output_file = 'cities_in_outage_area.csv'
        if len(cities_in_polygon) > 0:
            cities_in_polygon.to_csv(output_file, index=False)
            print(f"\nFound {len(cities_in_polygon)} cities within the outage area")
            print(f"Results saved to {output_file}")
            
            # Print the cities found
            print("\nCities found in the outage area:")
            for _, city in cities_in_polygon.iterrows():
                print(f"{city['name']} (Population: {city['population']})")
        else:
            print("\nNo cities found within the exact polygon boundary.")
            
            # Let's check for cities near the polygon
            buffer_distance = 0.1  # approximately 11km
            buffered_polygon = polygon_gdf.geometry.buffer(buffer_distance)
            buffered_gdf = gpd.GeoDataFrame(geometry=buffered_polygon)
            cities_near = cities_gdf[cities_gdf.geometry.within(buffered_polygon.iloc[0])]
            
            if len(cities_near) > 0:
                print("\nCities found near the outage area (within ~11km):")
                for _, city in cities_near.iterrows():
                    print(f"{city['name']} (Population: {city['population']})")
            else:
                print("No cities found even in the nearby area.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 