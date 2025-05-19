import asyncio
import pandas as pd
from pathlib import Path

from scrapper2 import main_scraper_flow 

   

def ensure_input_file(input_file):
    # Check if Input.xlsx exists, if not, create a dummy one for testing
    if not Path(input_file).exists():
        print(f"Warning: {input_file} not found. Creating a dummy file for testing.")
        dummy_data = {
            'URL_ID': ['TestID1', 'TestID2'],
            'URL': [
                "https://blinkit.com/cn/dairy-breakfast/bread-pav/cid/14/953",
                'https://www.zeptonow.com/cn/packaged-food/noodles-vermicelli/cid/5736ad99-f589-4d58-a24b-a12222320a37/scid/d5fbe386-0579-4461-b88b-af427ffb31ea'
            ]
        }
        dummy_df = pd.DataFrame(dummy_data)
        dummy_df.to_excel(input_file, index=False)
        print(f"Dummy {input_file} created with test URLs.")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Scrape article content from URLs in an Excel file.')
    parser.add_argument('--input', '-i', default='urls.xlsx', help='Path to the Excel file containing URLs.')
    parser.add_argument('--output', '-o', default='articles', help='Directory to save the article text files.')
    parser.add_argument('--no-cache', action='store_true', help='Disable caching for all requests.')
    parser.add_argument('--ignore-robots', action='store_true', help='Ignore robots.txt restrictions.')

    args = parser.parse_args()

    print(f"Starting article scraper with input file: {args.input} and output directory: {args.output}")
    print(f"Cache disabled: {args.no_cache}")
    print(f"Ignoring robots.txt: {args.ignore_robots}")

    INPUT_EXCEL_FILE = args.input
    SCRAPPER_OUTPUT_DIR = args.output
    OUTPUT_DIR = "output"

    # Ensure input file exists
    ensure_input_file(INPUT_EXCEL_FILE)

    # Step 1: Run the web scraper (async)
    asyncio.run(main_scraper_flow(
        input_file_path=INPUT_EXCEL_FILE, 
        output_directory=SCRAPPER_OUTPUT_DIR,
        disable_cache=args.no_cache,
        disable_robots_txt=args.ignore_robots
        ))
    
    print(f"Scraping completed. Check the '{SCRAPPER_OUTPUT_DIR}' directory for output files.")

