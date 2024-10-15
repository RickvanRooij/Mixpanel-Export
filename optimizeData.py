import json

def optimize_organization_data(input_file, output_file):
    try:
        # Read the standardized JSON file
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Create a dictionary with name as key and id as value
        optimized_data = {item['name'].replace('\\', ''): item['id'] for item in data}
        
        # Write the optimized data to a new file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(optimized_data, f, ensure_ascii=False, indent=2)
        
        print(f"Optimized data written to {output_file}")
    
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        print(f"Error occurred near: {e.doc[max(0, e.pos-20):e.pos+20]}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Usage
input_file = './reference/raw_and_standardized/standardized_channels.json'
output_file = './reference/optimized_channels.json'
optimize_organization_data(input_file, output_file)