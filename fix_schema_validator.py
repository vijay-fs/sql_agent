#!/usr/bin/env python
"""Script to fix syntax errors in schema_validator.py"""

import re

file_path = '/Users/ghost/Desktop/Coffee/PROTOTYPES/sql_agent_project/database/schema_validator.py'

# Read the file
with open(file_path, 'r') as f:
    content = f.read()

# Fix the problematic line with properly escaped quotes in regex
pattern = r'table_match = re.search\(r"table \[\'"\]\?\(\[.*?\]\) \(\?:not found\|doesn\'t exist\)",'
replacement = r'table_match = re.search(r"table [\'\\"]?([^\'\\\"\\s]+)[\'\\"]?(?:\\S+)? (?:not found|doesn\'t exist)",'

fixed_content = re.sub(pattern, replacement, content)

# Write the fixed content back to the file
with open(file_path, 'w') as f:
    f.write(fixed_content)

print("Fixed the schema_validator.py file successfully!")
