import asyncio
import json
import os
import csv
import time
import re
import hashlib
from dotenv import load_dotenv
from typing import List, Optional
from pydantic import BaseModel, TypeAdapter
from crawl4ai import (
    AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig, LLMConfig
)
from crawl4ai.extraction_strategy import LLMExtractionStrategy

load_dotenv()
key = os.getenv("OPENROUTER_KEY")
if key:
    print(f"API key loaded: {key[:4]}...{key[-4:]}")
else:
    print("No API key found!")


URL = "https://blinkit.com/cn/dairy-breakfast/bread-pav/cid/14/953"

class Product(BaseModel):
    title: str
    weight: Optional[str] = None
    price: str
    badge: Optional[str] = None
    reviews: Optional[str] = None
    
# Revised instruction prompt
instruction_llm = """Extract product data as JSON array with strict schema:
[
  {
    "title": "Product Name (exact from page)",
    "weight": "Extracted weight (e.g., '400g')",
    "price": "₹XX.XX (numeric only)",
    "badge": "Promotional text/null",
    "reviews": "Star rating/text/null"
  }
]

Rules:
1. Convert all prices to ₹ format
2. Extract weights from titles when separate field missing
3. Return empty array if no products found
4. Skip malformed entries
5. Never add explanatory text"""

# Enhanced product validation
def validate_products(raw_data: list) -> List[Product]:
    validated = []
    error_log = []
    
    for idx, item in enumerate(raw_data):
        try:
            # Convert all values to strings
            sanitized = {k: str(v) for k, v in item.items()}
            
            # Price normalization
            if 'price' in sanitized:
                sanitized['price'] = re.sub(r'[^\d.]', '', sanitized['price'])
                
            # Weight extraction fallback
            if not sanitized.get('weight'):
                if match := re.search(r'(\d+\s*[gG]|\d+\s*[kK][gG])', sanitized.get('title', '')):
                    sanitized['weight'] = match.group(1)
                    
            product = Product(**sanitized)
            validated.append(product)
        except Exception as e:
            error_log.append({
                "index": idx,
                "item": item,
                "error": str(e)
            })
    
    if error_log:
        with open("validation_errors.json", "w", encoding="utf-8") as f:
            json.dump(error_log, f, indent=2)
    
    return validated

async def main():
    # Enhanced retry logic with exponential backoff
    max_retries = 7
    base_delay = 8
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            async with AsyncWebCrawler(config=BrowserConfig(
                headless=True,
                stealth_mode=True,
                javascript_enabled=True,
                block_resources=["image", "stylesheet"],
                viewport_width=1280,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                extra_args={
                    "--disable-blink-features": "AutomationControlled"
                }
            )) as crawler:
                run_cfg = CrawlerRunConfig(
                    cache_mode=CacheMode.SMART,
                    cache_expiry=3600,  # 1 hour
                    cache_key=hashlib.md5(URL.encode()).hexdigest()
                )
                res = await crawler.arun(url=URL, config=run_cfg)
                
                # Response validation
                if getattr(res, "status_code", None) == 429:
                    raise Exception("Rate limit exceeded")
                
                # Process response
                process_extraction_results(res.extracted_content)
                break
                
        except Exception as e:
            retry_count += 1
            delay = min(120, base_delay * (2 ** retry_count))
            
            if "rate limit" in str(e).lower():
                print(f"Rate limited. Retrying in {delay}s (Attempt {retry_count})")
                await asyncio.sleep(delay)
            else:
                print(f"Error: {e}")
                if retry_count < max_retries:
                    print(f"Retrying in {delay}s")
                    await asyncio.sleep(delay)
                else:
                    print("Max retries exceeded")
                    raise

def scroll_script():
    """Return JavaScript to scroll the page and load all products"""
    return """
    (async () => {
        let scrollCount = 0;
        const maxScrolls = 12;  // Increase scrolls to capture more products
        const scrollInterval = setInterval(() => {
            window.scrollBy(0, window.innerHeight * 0.8);
            scrollCount++;
            if(scrollCount >= maxScrolls) clearInterval(scrollInterval);
        }, 1500);
        await new Promise(r => setTimeout(r, (maxScrolls + 2) * 1500));
    })();
    """

def process_extraction_results(content: str):
    """Process the extraction results with enhanced error handling"""
    try:
        # Enhanced JSON extraction
        if isinstance(content, str):
            # Handle string-wrapped JSON responses
            if content.startswith('"') and content.endswith('"'):
                content = content[1:-1].replace('\\"', '"')
            # Attempt JSON parsing
            try:
                data = json.loads(content)
            except json.JSONDecodeError:
                # Handle LLM's markdown-style JSON responses
                md_match = re.search(r'```json\s*(\{.*?\})\s*```', content, re.DOTALL)
                if md_match:
                    data = json.loads(md_match.group(1))
                else:
                    raise ValueError("No valid JSON found in response")
        else:
            # If content is already a dict/list
            data = content

        # Handle different possible response structures
        if isinstance(data, dict):
            if "products" in data:
                data = data["products"]
            elif "blocks" in data and isinstance(data["blocks"], list):
                if len(data["blocks"]) > 0:
                    data = data["blocks"]

        # Print parsed data
        print("\n[PARSED DATA]")
        print(json.dumps(data, indent=2))

        # Extract and normalize products
        raw_products = []
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    continue
                
                # Make sure we have title
                if "title" not in item:
                    continue
                    
                # Handle price field which might be missing or malformed
                if "price" not in item:
                    # Look for price in keys or values that contain ₹ symbol
                    for k, v in item.items():
                        if isinstance(k, str) and "₹" in k:
                            item["price"] = k
                            break
                        elif isinstance(v, str) and "₹" in v:
                            item["price"] = v
                            break
                
                # If we still don't have price, skip this item
                if "price" not in item:
                    continue
                
                # Ensure price is a string
                if not isinstance(item["price"], str):
                    item["price"] = str(item["price"])
                    
                raw_products.append(item)
                
        print(f"\nFound {len(raw_products)} raw products")

        # Validate with Pydantic
        try:
            adapter = TypeAdapter(List[Product])
            products = adapter.validate_python(raw_products)
            print(f"Validated {len(products)} products")
        except Exception as e:
            print("Validation Error:", e)
            # Manual validation as fallback
            products = []
            for item in raw_products:
                try:
                    product = Product(**item)
                    products.append(product)
                except Exception:
                    pass  # Skip invalid items
            print(f"After manual validation: {len(products)} products")
            
        if not products:
            print("No valid products found")
            return

        # Write CSV
        fieldnames = list(Product.model_json_schema()["properties"].keys())
        with open("products.csv", "w", newline="", encoding="utf-8") as fp:
            writer = csv.DictWriter(fp, fieldnames=fieldnames)
            writer.writeheader()
            for product in products:
                row = product.model_dump()
                for k in row:  # Handle None values
                    row[k] = row[k] or ""
                writer.writerow(row)

        print(f"\nSaved {len(products)} products to products.csv")
        
    except Exception as e:
        print("Error processing extraction results:", e)
        raise
