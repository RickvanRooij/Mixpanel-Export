import re
import json

def manual_json_fix(content):
    # Remove trailing commas
    content = re.sub(r',(\s*[\]}])', r'\1', content)
    
    # Fix escaped quotes
    content = re.sub(r'\\(["\'])', r'\1', content)
    
    # Ensure the content is wrapped in square brackets
    content = content.strip()
    if not content.startswith('['):
        content = '[' + content
    if not content.endswith(']'):
        content = content + ']'
    
    return content

def standardize_escapes(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Manually fix the JSON content
    fixed_content = manual_json_fix(content)

    try:
        # Parse the fixed JSON
        data = json.loads(fixed_content)

        # Write the standardized JSON
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"Standardized JSON written to {output_file}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        print(f"Error occurred near: {fixed_content[max(0, e.pos-20):e.pos+20]}")

# Usage
input_file = './reference/raw_and_standardized/channels_raw.json'
output_file = './reference/raw_and_standardized/standardized_channels.json'
standardize_escapes(input_file, output_file)