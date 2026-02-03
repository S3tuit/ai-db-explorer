#!/usr/bin/env python3
import sys
import re

# Usage:
# python3 py_utils/format_sorted_strings.py "bit_length char_length length"
# python3 py_utils/format_sorted_strings.py "var_pop" "var_samp" "variance"
# python3 py_utils/format_sorted_strings.py var_pop,var_samp,variance

def main() -> int:
    # Check if we have command-line arguments (excluding script name)
    if len(sys.argv) > 1:
        # Use command-line arguments
        data = ' '.join(sys.argv[1:])
    else:
        # Read from stdin
        if sys.stdin.isatty():
            print("Error: No input provided. Use stdin or pass arguments.", file=sys.stderr)
            print("\nUsage examples:", file=sys.stderr)
            print('  echo "func1 func2" | python3 format_sorted_strings.py', file=sys.stderr)
            print('  python3 format_sorted_strings.py "func1 func2"', file=sys.stderr)
            print('  python3 format_sorted_strings.py func1 func2 func3', file=sys.stderr)
            return 1
        data = sys.stdin.read()
    
    # Remove all quotes (both single and double)
    data = data.replace('"', '').replace("'", '')
    
    # Split by commas, whitespace, or newlines
    items = re.split(r'[,\s]+', data)
    
    # Filter out empty strings
    items = [s.strip() for s in items if s.strip()]
    
    if not items:
        print("Error: No valid items found in input.", file=sys.stderr)
        return 1
    
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
