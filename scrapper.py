import asyncio
import pandas as pd
import json
from pathlib import Path
from typing import List, Dict, Tuple, Optional

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy, JsonXPathExtractionStrategy
from crawl4ai.content_filter_strategy import PruningContentFilter
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator

class Loader:
    """Class to load URLs from an Excel file."""

    def __init__(self, file_path: str):
        """
        Initialize the Loader with the path to the Excel file.
        Args:
            file_path: Path to the Excel file containing URLs.
        """
        self.file_path = file_path

    def load_urls(self) -> List[Tuple[str, str]]:
        """
        Load URLs from the Excel file.
        The Excel file is expected to have 'URL_ID' and 'URL' columns.
        If not found, it falls back to using the first two columns.
        Returns:
            A list of tuples containing (URL_ID, URL).
        """
        try:
            df = pd.read_excel(self.file_path)

            # Check for specific column names (case-insensitive for robustness)
            url_id_col = None
            url_col = None

            for col in df.columns:
                if str(col).upper() == 'URL_ID':
                    url_id_col = col
                elif str(col).upper() == 'URL':
                    url_col = col

            if url_id_col and url_col:
                return list(zip(df[url_id_col].astype(str), df[url_col].astype(str)))
            elif len(df.columns) >= 2:
                print("Warning: Expected column names 'URL_ID' and 'URL' not found or not exactly matching.")
                print(f"Available columns: {df.columns.tolist()}")
                print(f"Using first column ('{df.columns[0]}') as URL_ID and second column ('{df.columns[1]}') as URL.")
                return list(zip(df.iloc[:, 0].astype(str), df.iloc[:, 1].astype(str)))
            else:
                print("Error: Excel file does not have at least two columns for URL_ID and URL.")
                return []

        except FileNotFoundError:
            print(f"Error: Input file not found at {self.file_path}")
            return []
        except Exception as e:
            print(f"Error loading URLs from {self.file_path}: {e}")
            return []


class ArticleScraper:
    """Class to scrape article content from URLs using Crawl4AI."""

    def __init__(self, disable_cache=False, disable_robots_txt=False):
        """
        Initialize the ArticleScraper.
        Args:
            disable_cache: If True, disable the cache for all crawls.
            disable_robots_txt: If True, ignore robots.txt restrictions.
        """
        # Create CSS extraction schema specifically targeting article content and title
        self.css_schema = {
            "name": "Article Content",
            "baseSelector": "article",  # Target the article container element
            "fields": [
                {
                    "name": "title",
                    "selector": "h1.entry-title",
                    "type": "text"
                },
                {
                    "name": "content",
                    "selector": ".td-container", 
                    "type": "html"  
                }
            ]
        }

        
        # Alternative schema for sites that don't use the article tag
        self.alt_schema = {
    "name": "E-commerce Product Catalog",
    "baseSelector": "div.category",
    # (1) We can define optional baseFields if we want to extract attributes 
    # from the category container
    "baseFields": [
        {"name": "data_cat_id", "type": "attribute", "attribute": "data-cat-id"}, 
    ],
    "fields": [
        {
            "name": "category_name",
            "selector": "h2.category-name",
            "type": "text"
        },
        {
            "name": "products",
            "selector": "plpContainer .product-item",  # Selector for product items
            "type": "nested_list",    # repeated sub-objects
            "fields": [
                {
                    "name": "name",
                    "selector": "h3.product-name",
                    "type": "text"
                },
                {
                    "name": "price",
                    "selector": "p.product-price",
                    "type": "text"
                },
                {
                    "name": "details",
                    "selector": "div.product-details",
                    "type": "nested",  # single sub-object
                    "fields": [
                        {
                            "name": "brand",
                            "selector": "span.brand",
                            "type": "text"
                        },
                        {
                            "name": "model",
                            "selector": "span.model",
                            "type": "text"
                        }
                    ]
                },
                {
                    "name": "features",
                    "selector": "ul.product-features li",
                    "type": "list",
                    "fields": [
                        {"name": "feature", "type": "text"} 
                    ]
                },
                {
                    "name": "reviews",
                    "selector": "div.review",
                    "type": "nested_list",
                    "fields": [
                        {
                            "name": "reviewer", 
                            "selector": "span.reviewer", 
                            "type": "text"
                        },
                        {
                            "name": "rating", 
                            "selector": "span.rating", 
                            "type": "text"
                        },
                        {
                            "name": "comment", 
                            "selector": "p.review-text", 
                            "type": "text"
                        }
                    ]
                },
                {
                    "name": "related_products",
                    "selector": "ul.related-products li",
                    "type": "list",
                    "fields": [
                        {
                            "name": "name", 
                            "selector": "span.related-name", 
                            "type": "text"
                        },
                        {
                            "name": "price", 
                            "selector": "span.related-price", 
                            "type": "text"
                        }
                    ]
                }
            ]
        }
    ]
}


        
        # Use CSS extraction strategies
        self.css_strategy = JsonCssExtractionStrategy(self.css_schema)
        self.alt_strategy = JsonCssExtractionStrategy(self.alt_schema)
        
        # Configure pruning filter to focus on article content and remove boilerplate
        self.prune_filter = PruningContentFilter(
            threshold=0.4,  # Lower threshold to include more real content
            threshold_type="dynamic",
            min_word_threshold=5  # Include paragraphs with at least 5 words
        )
        
        # Set up markdown generator with the pruning filter
        self.md_generator = DefaultMarkdownGenerator(content_filter=self.prune_filter)
        
        self.disable_cache = disable_cache
        self.disable_robots_txt = disable_robots_txt
        
        # Browser configuration
        self.browser_config = BrowserConfig(
            headless=True,
            java_script_enabled=True,
            text_mode=False,  # Changed to false to get full HTML structure
            # wait_until="networkidle2"  # Wait until network is idle
        )


    async def scrape_url(self, url: str) -> Optional[Dict[str, str]]:
        """
        Scrape article title and main content from a URL using Crawl4AI.
        Args:
            url: URL to scrape.
        Returns:
            A dictionary containing the title and content of the article,
            or None if scraping fails.
        """
        try:
            # Configure primary crawler to use article-specific extraction
            primary_config = CrawlerRunConfig(
                markdown_generator=self.md_generator,
                extraction_strategy=self.css_strategy,
                cache_mode=CacheMode.DISABLED if self.disable_cache else CacheMode.ENABLED,
                check_robots_txt=not self.disable_robots_txt,
                page_timeout=60000,
                verbose=True,
                # wait_for=".td-post-content",  # Wait specifically for content to load
                excluded_tags=["header", "footer", "nav", "aside", "menu"]
            )


            print(f"Starting crawl of {url}")
            
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                # First attempt with primary article tag strategy
                result = await crawler.arun(url=url, config=primary_config)
                
                print(f"Crawl of {url} completed with success={result.success}")
                
                if not result.success:
                    print(f"Error message: {result.error_message}")
                    return None
                
                # Try to extract content using the primary CSS strategy (article tag)
                article_data = None
                if result.extracted_content:
                    try:
                        extracted_json = json.loads(result.extracted_content)
                        
                        # Handle both dictionary and list return types
                        if isinstance(extracted_json, dict):
                            title = extracted_json.get("title", "")
                            content = extracted_json.get("content", "")
                            if title or content:
                                article_data = {"title": title, "content": content}
                                
                        elif isinstance(extracted_json, list) and len(extracted_json) > 0:
                            # Take first result if we get a list
                            first_item = extracted_json[0]
                            title = first_item.get("title", "")
                            content = first_item.get("content", "")
                            if title or content:
                                article_data = {"title": title, "content": content}
                    
                    except json.JSONDecodeError:
                        print(f"Failed to parse extracted_content as JSON for {url}")
                
                # If first strategy didn't work, try the alternative strategy
                if not article_data:
                    print(f"Primary strategy failed for {url}, trying alternative strategy")
                    
                    alt_config = CrawlerRunConfig(
                        markdown_generator=self.md_generator,
                        extraction_strategy=self.alt_strategy,
                        cache_mode=CacheMode.DISABLED if self.disable_cache else CacheMode.ENABLED,
                        check_robots_txt=not self.disable_robots_txt,
                        page_timeout=60000,
                        verbose=True,
                        excluded_tags=["header", "footer", "nav", "aside", "menu"]
                    )
                    
                    result = await crawler.arun(url=url, config=alt_config)
                    
                    if result.success and result.extracted_content:
                        try:
                            extracted_json = json.loads(result.extracted_content)
                            
                            if isinstance(extracted_json, dict):
                                title = extracted_json.get("title", "")
                                content = extracted_json.get("content", "")
                                if title or content:
                                    article_data = {"title": title, "content": content}
                                    
                            elif isinstance(extracted_json, list) and len(extracted_json) > 0:
                                first_item = extracted_json[0]
                                title = first_item.get("title", "")
                                content = first_item.get("content", "")
                                if title or content:
                                    article_data = {"title": title, "content": content}
                        
                        except json.JSONDecodeError:
                            print(f"Failed to parse alternative extracted_content as JSON for {url}")
                
                # Fall back to markdown if extraction strategies failed
                if not article_data:
                    print(f"Extraction strategies failed for {url}, falling back to markdown")
                    
                    # Try to get title from page metadata
                    title = result.title if hasattr(result, 'title') and result.title else "No Title Found"
                    
                    # Try fit_markdown first (filtered content), then raw_markdown if needed
                    content = None
                    if hasattr(result.markdown, 'fit_markdown') and result.markdown.fit_markdown:
                        content = result.markdown.fit_markdown
                    elif hasattr(result.markdown, 'raw_markdown') and result.markdown.raw_markdown:
                        content = result.markdown.raw_markdown
                    else:
                        print(f"No markdown content available for {url}")
                        return None
                    
                    article_data = {
                        'title': title,
                        'content': content
                    }
                
                return article_data
                
        except Exception as e:
            print(f"Exception while scraping URL {url}: {e}")
            import traceback
            traceback.print_exc()
            return None

    async def scrape_urls(self, urls_with_ids: List[Tuple[str, str]]) -> Dict[str, Optional[Dict[str, str]]]:
        """
        Scrape article content from multiple URLs.
        Args:
            urls_with_ids: A list of tuples containing (URL_ID, URL).
        Returns:
            A dictionary mapping URL_IDs to article content (or None if failed for a URL).
        """
        results = {}
        for url_id, url in urls_with_ids:
            print(f"Scraping {url_id}: {url}")
            article_data = await self.scrape_url(url)
            results[url_id] = article_data
        
        return results


class Saver:
    """Class to save article content to text files."""

    def __init__(self, output_dir: str):
        """Initialize the Saver with the output directory."""
        self.output_dir_path = Path(output_dir)
        self.output_dir_path.mkdir(parents=True, exist_ok=True)

    def save_article(self, url_id: str, article_data: Optional[Dict[str, str]]) -> None:
        """Save article content to a text file in Markdown format."""
        if article_data is None:
            print(f"Skipping save for {url_id} as no data was scraped.")
            file_path = self.output_dir_path / f"{url_id}_error.txt"
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(f"Failed to scrape content for URL_ID: {url_id}")
            return

        try:
            title = article_data.get('title', 'No Title Provided')
            content = article_data.get('content', 'No Content Provided')
            
            formatted_content = f"# {title}\n\n{content}"
            
            file_path = self.output_dir_path / f"{url_id}.txt"
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(formatted_content)
            
            print(f"Article {url_id} saved to {file_path}")
        except Exception as e:
            print(f"Error saving article {url_id}: {e}")

    def save_articles(self, articles_map: Dict[str, Optional[Dict[str, str]]]) -> None:
        """Save multiple articles to text files."""
        for url_id, article_data in articles_map.items():
            self.save_article(url_id, article_data)


async def main_scraper_flow(input_file_path: str, output_directory: str, disable_cache=False, disable_robots_txt=False) -> None:
    """Main flow to run the scraper: load URLs, scrape articles, and save them."""
    loader = Loader(input_file_path)
    scraper = ArticleScraper(disable_cache=disable_cache, disable_robots_txt=disable_robots_txt)
    saver = Saver(output_directory)

    urls_to_scrape = loader.load_urls()
    if not urls_to_scrape:
        print("No URLs loaded. Exiting.")
        return

    print(f"Loaded {len(urls_to_scrape)} URLs to scrape.")
    scraped_articles_map = await scraper.scrape_urls(urls_to_scrape)
    saver.save_articles(scraped_articles_map)

    successful_scrapes = sum(1 for article in scraped_articles_map.values() if article is not None)
    print(f"\nScraping process completed.")
    print(f"Successfully scraped and saved {successful_scrapes} out of {len(urls_to_scrape)} articles.")
