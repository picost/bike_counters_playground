#!/usr/bin/env python3
"""
HTML Indentation Module

This module provides functionality to reformat minified HTML files with proper indentation.
It takes a minified HTML file and outputs a nicely formatted version with 2-space indentation.

Usage:
    python indent_html_data.py <html_file>

Example:
    python indent_html_data.py ecoucounter_example.html

Author: Generated for HTML formatting
Date: 2025-12-01
"""

import re
import sys
from pathlib import Path


def format_html(html: str) -> str:
    """
    Format HTML content with proper indentation.
    
    This function takes minified HTML and adds newlines and indentation
    to make it human-readable in an editor.
    
    Parameters
    ----------
    html : str
        The HTML content to format (can be minified)
        
    Returns
    -------
    str
        Formatted HTML with proper 2-space indentation
        
    Examples
    --------
    >>> html = '<html><body><p>Hello</p></body></html>'
    >>> formatted = format_html(html)
    >>> print(formatted)
    <html>
      <body>
        <p>
          Hello
        </p>
      </body>
    </html>
    """
    # Add newlines after tags
    html = re.sub(r'>([^<\s])', r'>\n\1', html)
    html = re.sub(r'(<[^/>][^>]*>)', r'\1\n', html)
    html = re.sub(r'(</[^>]+>)', r'\1\n', html)
    
    # Add indentation
    lines = html.split('\n')
    formatted = []
    indent = 0
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Decrease indent for closing tags
        if line.startswith('</'):
            indent = max(0, indent - 1)
        
        # Add indented line
        formatted.append('  ' * indent + line)
        
        # Increase indent for opening tags (but not self-closing or closing tags)
        if line.startswith('<') and not line.startswith('</') and not line.endswith('/>') and not line.startswith('<!'):
            # Check if it's not a self-closing tag
            tag_name = line.split()[0].replace('<', '').replace('>', '')
            if tag_name.lower() not in ['meta', 'link', 'img', 'br', 'hr', 'input']:
                indent += 1
    
    return '\n'.join(formatted)


def indent_html_file(filepath: str) -> None:
    """
    Read an HTML file, format it, and write it back.
    
    Parameters
    ----------
    filepath : str
        Path to the HTML file to format
        
    Raises
    ------
    FileNotFoundError
        If the specified file does not exist
    PermissionError
        If the file cannot be read or written
        
    Examples
    --------
    >>> indent_html_file('ecoucounter_example.html')
    ✓ HTML file reformatted with proper indentation!
    Total lines: 301
    """
    file_path = Path(filepath)
    
    # Check if file exists
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    
    # Read the HTML file
    with open(file_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    # Format the HTML
    formatted_html = format_html(html_content)
    
    # Write back to the file
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(formatted_html)
    
    # Print success message
    num_lines = len(formatted_html.split('\n'))
    print(f"✓ HTML file reformatted with proper indentation!")
    print(f"File: {filepath}")
    print(f"Total lines: {num_lines}")


def main():
    """
    Main entry point for command-line usage.
    """
    if len(sys.argv) != 2:
        print("Usage: python indent_html_data.py <html_file>")
        print("\nExample:")
        print("  python indent_html_data.py ecoucounter_example.html")
        sys.exit(1)
    
    html_file = sys.argv[1]
    
    try:
        indent_html_file(html_file)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except PermissionError as e:
        print(f"Error: Permission denied - {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
