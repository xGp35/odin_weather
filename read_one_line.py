import csv

# Open the input file for reading
with open('output_parsed.csv', 'r') as input_file:
    # Create a CSV reader
    reader = csv.reader(input_file)
    
    # Read the header and first data row
    header = next(reader)
    first_row = next(reader)
    
    # Open the output file for writing
    with open('one_line.csv', 'w', newline='') as output_file:
        # Create a CSV writer
        writer = csv.writer(output_file)
        
        # Write the header and first row
        writer.writerow(header)
        writer.writerow(first_row)

print("Successfully created one_line.csv with one line of data") 