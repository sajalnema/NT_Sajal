import json

# Open the file
f = open('training.json', 'r')
data = json.load(f)
# Close the file
f.close()

# Extract data from JSON
name = data["name"]
date = data["date"]
completed = data["completed"]
instructor_name = data["instructor"]["name"]
instructor_website = data["instructor"]["website"]
participants = data["participants"]

# Print the extracted data
print("name: ", name)
print("date: ", date)
print("completed: ", completed)
print("instructor name: ", instructor_name)
print("instructor website: ", instructor_website)
print("participants:")
for p in participants:
    print("name: ", p["name"])
    print("email: ", p["email"])