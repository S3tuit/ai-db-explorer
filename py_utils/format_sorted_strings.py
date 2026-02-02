#!/usr/bin/env python3
import sys
import re

# Usage:
# echo "bit_length char_length length" | python3 py_utils/format_sorted_strings.py
# echo '"var_pop", "var_samp", "variance"' | python3 py_utils/format_sorted_strings.py
# echo 'var_pop,var_samp,variance' | python3 py_utils/format_sorted_strings.py

def main() -> int:
    data = sys.stdin.read()
    
    # Remove all quotes (both single and double)
    data = data.replace('"', '').replace("'", '')
    
    # Split by commas, whitespace, or newlines
    items = re.split(r'[,\s]+', data)
    
    # Filter out empty strings
    items = [s.strip() for s in items if s.strip()]
    
    # Remove duplicates and sort
    items = sorted(set(items))
    
    print("{")
    for i, s in enumerate(items):
        comma = "," if i + 1 < len(items) else ""
        print(f'  "{s}"{comma}')
    print("}")
    
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
