import requests
from datetime import datetime, timezone, timedelta
import json
import base64

# Mixpanel API credentials
API_SECRET = '9410d476c49cb45ee50fa92aa6f542d5'
# Add this if you have a project ID, otherwise leave as is
PROJECT_ID = 'YOUR_PROJECT_ID'

# Set start date to januari 1, 2024, at 00:01 hours UTC
start_date = datetime(2024, 8, 1, 0, 0, 1, tzinfo=timezone.utc)

# Set end date to august 1 20204 at 00:00 UTC
end_date = datetime(2024, 8, 1, 0, 0, 0, tzinfo=timezone.utc)

# Prepare request
url = 'https://data-eu.mixpanel.com/api/2.0/export'  # Note: Using EU data location
params = {
    'from_date': start_date.strftime('%Y-%m-%d'),
    'to_date': end_date.strftime('%Y-%m-%d')
}

# Add project ID to params if available
if PROJECT_ID != 'YOUR_PROJECT_ID':
    params['project_id'] = PROJECT_ID

# Authenticate and make request
auth = base64.b64encode(API_SECRET.encode('utf-8')).decode('utf-8')
headers = {'Authorization': f'Basic {auth}'}

print(f"Sending request to Mixpanel API for date range: {
      start_date.date()} to {end_date.date()}")
response = requests.get(url, params=params, headers=headers, stream=True)

print(f"Received response with status code: {response.status_code}")
print("Response headers:")
for header, value in response.headers.items():
    print(f"{header}: {value}")

if response.status_code != 200:
    print(f"Error: Received status code {response.status_code}")
    print(response.text)
    exit(1)


def get_new_filename(base_name, file_number):
    return f"{base_name}_{file_number:03d}.json"


# Process the streamed response
print("\nProcessing and saving data...")
event_count = 0
filtered_count = 0
file_number = 37
events_per_file = 1000000
current_file = None

with open('mixpanel_export.json', 'w') as f:
    for line in response.iter_lines():
        if line:
            try:
                # Parse the JSON object
                event = json.loads(line)

                # Check if distinct_id contains "staging" or "development"
                distinct_id = event.get('properties', {}).get(
                    'distinct_id', '').lower()
                if 'staging' in distinct_id or 'development' in distinct_id:
                    filtered_count += 1
                    continue  # Skip this event

                # Open a new file if needed
                if event_count % events_per_file == 0:
                    if current_file:
                        current_file.close()
                    filename = get_new_filename("mixpanel_export", file_number)
                    current_file = open(filename, 'w')
                    file_number += 1
                    print(f"\nCreated new file: {filename}")

                # Write the event to file
                json.dump(event, current_file)
                current_file.write('\n')
                event_count += 1

                # Print progress every 1000 events
                if event_count % 1000 == 0:
                    print(f"Processed {
                          event_count + filtered_count} events (Saved: {event_count}, Filtered: {filtered_count})...")
            except json.JSONDecodeError:
                print(
                    f"Warning: Received non-JSON data: {line.decode('utf-8')}")

print(f"\nExport complete.")
print(f"Total events processed: {event_count + filtered_count}")
print(f"Events saved: {event_count}")
print(f"Events filtered out: {filtered_count}")
print(f"Data saved to mixpanel_export.json")
print(f"Date range: {start_date.strftime('%Y-%m-%d %H:%M:%S')
                     } to {end_date.strftime('%Y-%m-%d %H:%M:%S')} UTC")
