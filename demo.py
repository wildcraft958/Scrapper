import asyncio
import json
import os
import csv
import time
from dotenv import load_dotenv
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, TypeAdapter
from crawl4ai import (
    AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig, LLMConfig
)
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from litellm import completion                       

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
    
instruction_llm = """
Extract all bread and pav products from the webpage.
For each product, extract these fields:
- title: The product name
- weight: Weight or quantity information
- price: Price with currency symbol
- badge: Any promotional badge (if available)
- reviews: Review information (if available)

Return a JSON array where each item is a product object.
Example:
[
  {
    "title": "Product Name",
    "weight": "400g",
    "price": "Rs.50",
    "badge": null,
    "reviews": null
  }
]
"""

# 1) A little helper to wrap raw strings
class OpenRouterWrapper:
    def __init__(self, model, **default_kwargs):
        self.model = model
        self.default_kwargs = default_kwargs

    def startswith(self, prefix):
        return self.model.startswith(prefix)

    def __call__(self, *, messages, transforms=None, route=None, **extra_args):
        raw_text = completion(
            model=self.model,
            messages=messages,
            transforms=transforms,
            route=route,
            **{**self.default_kwargs, **extra_args},
        )
        class Choice:
            def __init__(self, text):
                self.text = text

        class FakeResp:
            def __init__(self, text):
                self.choices = [Choice(text)]

        return FakeResp(raw_text)

async def main():
    # Use smaller model to avoid rate limits

    # Configure LLM via Litellm wrapper
    my_llm_client = OpenRouterWrapper(
        model="openrouter/deepseek/deepseek-r1:free",
        temperature=0.0,
        max_tokens=2000,
    )
    llm_cfg = LLMConfig(
        provider=my_llm_client,                           
        api_token=os.getenv("OPENROUTER_KEY"),
    )
    
    # combined with tool/function calling
    strategy = LLMExtractionStrategy(
        llm_config=llm_cfg,
        extraction_type="block",  # Use block instead of schema
        instruction=instruction_llm,
        chunk_token_threshold=1200,
        overlap_rate=0.1,
        apply_chunking=True,
        input_format="markdown",
        extra_args={
            "temperature": 0.0,
            "max_tokens": 2000,
        },
        retries=3,
    )

    crawl_cfg = CrawlerRunConfig(
        extraction_strategy=strategy,
        cache_mode=CacheMode.BYPASS,
        process_iframes=False,
        remove_overlay_elements=True,
        exclude_external_links=True,
        js_code=[scroll_script()],
        wait_for="js:() => new Promise(r => setTimeout(r, 3000))" 
    )

    # Add retry logic for rate limiting
    max_retries = 5
    retry_count = 0
    base_delay = 5 
    
    while retry_count < max_retries:
        try:
            async with AsyncWebCrawler(config=BrowserConfig(headless=True, verbose=True)) as crawler:
                res = await crawler.arun(url=URL, config=crawl_cfg)
                
                # Print the full response for debugging
                print("\n[RAW RESPONSE]")
                print(res.extracted_content)
                
                if not res.success:
                    print("Error:", res.error_message)
                    raise Exception(res.error_message)

                # Save raw response for debugging
                with open("debug_output.json", "w") as f:
                    f.write(res.extracted_content)

                # Process the extraction results
                process_extraction_results(res.extracted_content)
                break  # Success, exit the retry loop
                
        except Exception as e:
            retry_count += 1
            if "rate limit" in str(e).lower() and retry_count < max_retries:
                wait_time = min(60, base_delay * (2 ** retry_count))  # Exponential backoff
                print(f"Rate limit error. Waiting {wait_time} seconds before retrying...")
                await asyncio.sleep(wait_time)
            elif retry_count < max_retries:
                print(f"Error: {e}. Retrying... (Attempt {retry_count}/{max_retries})")
                await asyncio.sleep(base_delay)
            else:
                print(f"Failed after {max_retries} attempts: {e}")
                break

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
    """Process the extraction results and save to CSV"""
    try:
        # Try to parse JSON
        data = json.loads(content)
        
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
                    # Look for price in keys or values that contain â‚¹ symbol
                    for k, v in item.items():
                        if isinstance(k, str) and "â‚¹" in k:
                            item["price"] = k
                            break
                        elif isinstance(v, str) and "â‚¹" in v:
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
        
    except json.JSONDecodeError as e:
        print("JSON Error:", e)
        print("Raw content:", content)

if __name__ == "__main__":
    asyncio.run(main())