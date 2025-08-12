#!/usr/bin/env python3
"""
Test script for the automated clean_up function
"""

import re

def clean_up(generated_sql):
    """
    Automatically clean up backtick-quoted column names in SQL queries.
    Finds all `backtick quoted` substrings and converts them to clean identifiers.
    
    Examples:
    `FREE MEAL COUNT (K-12)` -> FREE_MEAL_COUNT_K12
    `ENROLLMENT (AGES 5-17)` -> ENROLLMENT_AGES_5_17
    `PERCENT (%) ELIGIBLE FREE (K-12)` -> PERCENT_ELIGIBLE_FREE_K12
    """
    
    def transform_backtick_content(match):
        """Transform the content inside backticks to a clean identifier."""
        content = match.group(1)  # Get content between backticks
        
        # Convert to uppercase
        content = content.upper()
        
        # Replace spaces with underscores
        content = content.replace(' ', '_')
        
        # Replace hyphens with underscores (e.g., "K-12" -> "K_12", "5-17" -> "5_17")
        content = content.replace('-', '_')
        
        # Remove non-alphanumeric characters except underscores
        # This removes parentheses, percent signs, slashes, etc.
        content = re.sub(r'[^A-Z0-9_]', '', content)
        
        # Clean up multiple consecutive underscores
        content = re.sub(r'_+', '_', content)
        
        # Remove leading/trailing underscores
        content = content.strip('_')
        
        return content
    
    # Find all backtick-quoted substrings and replace them
    # Pattern matches `anything inside backticks`
    pattern = r'`([^`]+)`'
    
    # Replace each backtick-quoted substring with its cleaned version
    generated_sql = re.sub(pattern, transform_backtick_content, generated_sql)
    
    return generated_sql

if __name__ == "__main__":
    # Test cases based on your examples
    test_cases = [
        "`FREE MEAL COUNT (K-12)`",
        "`FREE MEAL COUNT (AGES 5-17)`", 
        "`ENROLLMENT (K-12)`",
        "`ENROLLMENT (AGES 5-17)`",
        "`PERCENT (%) ELIGIBLE FREE (K-12)`",
        "`PERCENT (%) ELIGIBLE FREE (AGES 5-17)`",
        "`COUNT (K-12)`",
        "`COUNT (AGES 5-17)`",
        "`SCHOOL NAME`",
        "`CHARTER SCHOOL (Y/N)`",
        "SELECT `FREE MEAL COUNT (K-12)` FROM FRPM WHERE `ENROLLMENT (AGES 5-17)` > 100"
    ]
    
    print("Testing automated cleanup function:")
    print("=" * 60)
    
    for test_case in test_cases:
        result = clean_up(test_case)
        print(f"Input:  {test_case}")
        print(f"Output: {result}")
        print("-" * 40)