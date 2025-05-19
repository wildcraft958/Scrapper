import asyncio
import pandas as pd
from pathlib import Path
import argparse
import re
from pathlib import Path
import json
import csv
import os
from dotenv import load_dotenv
from openai import OpenAI

from scrapper2 import main_scraper_flow 

load_dotenv()

client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv("OPENROUTER_KEY"),
)
   

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
    
    # Remove links from all scraped text files
    for txt_file in Path(SCRAPPER_OUTPUT_DIR).glob("*.txt"):
        content = txt_file.read_text(encoding="utf-8")
        cleaned = re.sub(r"https?://\S+|www\.\S+", "", content)
        txt_file.write_text(cleaned, encoding="utf-8")
        print(f"Removed links from {txt_file.name}")

    print(f"Scraping completed. Check the '{SCRAPPER_OUTPUT_DIR}' directory for output files.")

    # LLM-based data extraction and CSV conversion

    instruction_llm = """
    Extract all bread and pav products from the webpage.
    For each product, extract these fields:
    - title: The product name
    - weight: Weight or quantity information
    - description: Description of the product
    - discount: Discount information (if available)
    - price: Price with currency symbol
    - badge: Any promotional badge (if available)
    - reviews: Review information (if available)

    Return a JSON array where each item is a product object. Ensure the output is valid JSON and avoid any additional text.
    Example:
    [
    {
        "title": "Product Name",
        "weight": "400g",
        "description": "Description of the product",
        "discount": "10% off",
        "price": "₹50",
        "badge": null,
        "reviews": null
    }
    ]
    """

    all_products = []
    for txt_file in Path(SCRAPPER_OUTPUT_DIR).glob("*.txt"):
        batch_text = txt_file.read_text(encoding="utf-8")
        if not batch_text.strip():
            print(f"Skipping empty file: {txt_file.name}")
            continue
        
        try:
            response = client.chat.completions.create(
                model="deepseek/deepseek-r1:free",
                messages=[
                    {"role": "system", "content": instruction_llm},
                    {"role": "user",   "content": batch_text},
                ],
            )
        except Exception as e:
            print(f"API call failed for {txt_file.name}: {e}")
            continue
        
        content = response.choices[0].message.content or ""
        print("LLM raw response:", repr(content))
        
        # Remove markdown fences if present
        if content.startswith("```json"):
            content = content[7:-3].strip()  # Remove ```json and trailing ```
        elif content.startswith("```"):
            content = content[3:-3].strip()
        
        try:
            batch_products = json.loads(content)
            all_products.extend(batch_products)
        except json.JSONDecodeError as e:
            print(f"JSON parsing failed for {txt_file.name}: {e}")

    # Check if combined processing is necessary
    combined_text = "\n".join([
        txt_file.read_text(encoding="utf-8")
        for txt_file in Path(SCRAPPER_OUTPUT_DIR).glob("*.txt")
    ])

    if combined_text.strip():
        try:
            response = client.chat.completions.create(
                model="deepseek/deepseek-r1:free",
                messages=[
                    {"role": "system", "content": instruction_llm},
                    {"role": "user", "content": combined_text},
                ],
            )
            content = response.choices[0].message.content or ""
            # Remove markdown fences
            if content.startswith("```json"):
                content = content[7:-3].strip()
            elif content.startswith("```"):
                content = content[3:-3].strip()
            combined_products = json.loads(content)
            all_products.extend(combined_products)
        except Exception as e:
            print(f"Combined API call failed: {e}")
    else:
        print("No combined text available for processing.")

    # Remove duplicates (if any)
    unique_products = [dict(t) for t in {tuple(d.items()) for d in all_products}]

    # Save to CSV
    csv_path = Path(SCRAPPER_OUTPUT_DIR) / "products.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["title", "weight", "description", "discount", "price", "badge", "reviews"])
        writer.writeheader()
        writer.writerows(unique_products)

    print(f"Extracted product data saved to {csv_path}")