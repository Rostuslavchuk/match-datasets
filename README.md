Approach

    Extract: Loading data from CSV files into Pandas DataFrames for efficient processing.

    Normalization: Cleaning company names by removing legal suffixes (Inc, Ltd, etc.) and special characters.
    Converting all text to lowercase.
    Standardizing ZIP codes and creating a full_address column for consistent comparison.

    Transform: Grouping data by clean_name to combine all known locations for each company into a single list (handling one-to-many relationships).

    Match: Merging datasets based on the clean_name key.

    Fuzzy Address Validation: Using the Levenshtein Distance algorithm (via thefuzz library) to find overlapping locations. 
    With a threshold of 85%, the system successfully matches addresses despite differences like "St" vs "Street" or the presence of "Canada".

Data Quality Issues Found

    Inconsistent Formatting: One dataset included country names ("Canada") in addresses while the other did not.

    Legal Suffixes: Variations in legal forms (LP, Inc, LLC) prevented simple exact matching.

    Postal Codes: Different formats for ZIP codes (some with spaces, some without).

    One-to-Many Relationships: 3.15% of cases showed one brand name linked to multiple legal entities or branches.

Technical Features

    Logging: The system includes a custom logger (logger_config.py) that tracks every step of the process (Extraction, Transformation, Matching) and provides detailed metrics in the console.

    Scalability: The code uses an Abstract Base Class (ABC) structure, making it easy to adapt for new datasets in the future.

    Robustness: Added handling for missing values (NaN) and incorrect data types during normalization.


Calculated Metrics

    Match Rate: 43.07%

    Unmatched Records: 56.93%

    One-to-Many Matches: 3.15%

How to run

    Install dependencies: pip install -r requirements.txt

    Run the script: python main.py

    Check results: The final matched data will be saved in matching_results.csv. Detailed process logs will appear in your terminal.
