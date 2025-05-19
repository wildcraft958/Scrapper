import asyncio
from crawl4ai import AsyncWebCrawler
from openai import OpenAI
import os
from dotenv import load_dotenv

URL = "https://blinkit.com/cn/dairy-breakfast/bread-pav/cid/14/953"


async def main():
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(URL)
        return result.cleaned_html
        
load_dotenv()
print("OPENROUTER_KEY:", os.getenv("OPENROUTER_KEY") is not None)
        
client = OpenAI(
  base_url="https://openrouter.ai/api/v1",
  api_key=os.getenv("OPENROUTER_KEY"),
)

instruction_llm = """
Extract all bread and pav products from the webpage.
For each product, extract these fields:
- title: The product name
- weight: Weight or quantity information
- description: Description of the product
- discount: Discount information (if available)
- price: Price with currency symbol
- reviews: Review information (if available)

Return a JSON array where each item is a product object.
Example:
[
  {
    "title": "Product Name",
    "weight": "400g",
    "price": "₹50",
    "badge": null,
    "reviews": null
  }
]
"""

if __name__ == "__main__":
    scraped_html = asyncio.run(main())
    print("✅ Scraping done, length:", len(scraped_html))

    try:
        print("🚀 Sending to LLM…")
        completion = client.chat.completions.create(
          extra_body={},
          model="deepseek/deepseek-r1:free",
          messages=[{"role":"user","content":scraped_html + instruction_llm}]
        )
        print("✅ LLM returned, printing…")
        print(completion.choices[0].message.content)
    except Exception as e:
        print("❌ LLM call failed:", e)
