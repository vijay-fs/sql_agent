#!/usr/bin/env python
"""Script to fix the schema_validator.py file line by line"""

file_path = '/Users/ghost/Desktop/Coffee/PROTOTYPES/sql_agent_project/database/schema_validator.py'

with open(file_path, 'r') as f:
    lines = f.readlines()

# Find and replace the problematic line
for i, line in enumerate(lines):
    if "table_match = re.search" in line and "not found|doesn't exist" in line:
        lines[i] = '                table_match = re.search(r"table [\'\\"]?([^\'\\\"\\s]+)[\'\\"]?(?:\\S+)? (?:not found|doesn\'t exist)", str(e).lower())\n'

# Write the fixed content back to the file
with open(file_path, 'w') as f:
    f.writelines(lines)

print("Fixed the schema_validator.py file by directly replacing the problematic line.")
