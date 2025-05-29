#!/usr/bin/env python3
"""
Extract founders/makers data from ProductHunt product pages.
Reads posts.csv and outputs posts-with-founders.csv with makers data.
"""

import asyncio
import json
import re
import time
from pathlib import Path
from typing import Dict, Any, List, Optional

import click
import httpx
import pandas as pd
from tqdm.asyncio import tqdm


def extract_makers_object(html_content: str) -> Optional[str]:
    """
    Extract the makers object from HTML content and return as JSON string.
    """
    # Check if makers is anywhere in the HTML
    if '"makers":' not in html_content:
        return None
    
    # Find the makers object and extract the complete JSON
    pos = html_content.find('"makers":')
    if pos == -1:
        return None
    
    # Start parsing from after "makers":
    start_pos = pos + len('"makers":')
    json_str = html_content[start_pos:]
    
    # Find the opening brace
    brace_start = json_str.find('{')
    if brace_start == -1:
        return None
    
    # Track braces to find the complete object
    brace_count = 0
    in_string = False
    escape_next = False
    end_pos = brace_start
    
    for i in range(brace_start, len(json_str)):
        char = json_str[i]
        
        if escape_next:
            escape_next = False
            continue
            
        if char == '\\':
            escape_next = True
            continue
            
        if char == '"' and not escape_next:
            in_string = not in_string
            continue
            
        if not in_string:
            if char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0:
                    end_pos = i + 1
                    break
    
    if brace_count == 0:
        makers_json = json_str[brace_start:end_pos]
        try:
            # Validate it's proper JSON and handle Unicode properly
            parsed = json.loads(makers_json)
            # Re-encode to ensure proper formatting
            makers_json_clean = json.dumps(parsed, ensure_ascii=False, separators=(',', ':'))
            return makers_json_clean
        except json.JSONDecodeError:
            return None
    
    return None


class FoundersExtractor:
    def __init__(self, max_requests_per_second: int = 8):
        self.max_requests_per_second = max_requests_per_second
        self.request_times = []
        self.client = None
        
    async def __aenter__(self):
        self.client = httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0),
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()
    
    async def _rate_limit(self):
        """Ensure we don't exceed rate limits."""
        now = time.time()
        self.request_times = [t for t in self.request_times if now - t < 1.0]
        
        if len(self.request_times) >= self.max_requests_per_second:
            sleep_time = 1.0 - (now - self.request_times[0])
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
        
        self.request_times.append(now)
    
    async def fetch_product_page(self, ph_id: str) -> Optional[str]:
        """Fetch ProductHunt product page HTML."""
        url = f"https://producthunt.com/products/{ph_id}"
        
        try:
            await self._rate_limit()
            response = await self.client.get(url)
            
            if response.status_code == 200:
                return response.text
            elif response.status_code == 404:
                return None
            else:
                return None
                
        except Exception:
            return None


async def process_post(post_data: Dict[str, Any], extractor: FoundersExtractor) -> Dict[str, Any]:
    """Process a single post to extract founders data."""
    ph_id = str(post_data['ph_id'])
    
    # Fetch the product page
    html_content = await extractor.fetch_product_page(ph_id)
    
    if not html_content:
        return {**post_data, 'makers_object': None}
    
    # Extract makers object
    makers_json = extract_makers_object(html_content)
    
    return {
        **post_data,
        'makers_object': makers_json
    }


async def extract_founders_from_csv(input_file: Path, output_file: Path, rate_limit: int) -> None:
    """Extract founders data from posts CSV."""
    
    # Read existing CSV
    try:
        df = pd.read_csv(input_file)
        print(f"Read {len(df)} posts from {input_file}")
    except Exception as e:
        print(f"Error reading input file: {e}")
        return
    
    # Convert to list of dicts to preserve all original data
    posts = df.to_dict('records')
    
    # Check if output file exists and load already processed posts
    processed_ph_ids = set()
    if output_file.exists():
        try:
            existing_df = pd.read_csv(output_file)
            processed_ph_ids = set(str(ph_id) for ph_id in existing_df['ph_id'])
            print(f"Found {len(processed_ph_ids)} already processed posts")
        except Exception:
            pass
    
    # Filter out already processed posts
    posts_to_process = [post for post in posts if str(post['ph_id']) not in processed_ph_ids]
    
    print(f"Processing {len(posts_to_process)} posts (skipping {len(posts) - len(posts_to_process)} already processed)")
    
    if not posts_to_process:
        print("No posts to process!")
        return
    
    # Process posts with progress bar
    file_exists = output_file.exists()
    
    async with FoundersExtractor(max_requests_per_second=rate_limit) as extractor:
        with tqdm(total=len(posts_to_process), desc="Extracting makers") as pbar:
            for post_data in posts_to_process:
                result = await process_post(post_data, extractor)
                
                # Save incrementally with proper CSV quoting
                result_df = pd.DataFrame([result])
                if file_exists:
                    result_df.to_csv(output_file, mode='a', header=False, index=False, 
                                   encoding='utf-8', quoting=1)  # QUOTE_ALL
                else:
                    result_df.to_csv(output_file, mode='w', header=True, index=False, 
                                   encoding='utf-8', quoting=1)  # QUOTE_ALL
                    file_exists = True
                
                pbar.update(1)
                await asyncio.sleep(0.125)  # 8 req/sec = 0.125s delay
    
    print(f"\nCompleted! Results saved to {output_file}")


@click.command()
@click.option('--input', '-i', default='posts.csv', help='Input CSV file')
@click.option('--output', '-o', default='posts-with-founders.csv', help='Output CSV file')
@click.option('--rate-limit', '-r', default=8, help='Max requests per second')
def main(input: str, output: str, rate_limit: int):
    """
    Extract founders/makers data from ProductHunt product pages.
    """
    input_path = Path(input)
    output_path = Path(output)
    
    if not input_path.exists():
        click.echo(f"Error: Input file {input_path} not found", err=True)
        return
    
    click.echo(f"Extracting founders from {input_path} -> {output_path}")
    click.echo(f"Rate limit: {rate_limit} requests/second")
    
    asyncio.run(extract_founders_from_csv(input_path, output_path, rate_limit))


if __name__ == '__main__':
    main() 