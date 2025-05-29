#!/usr/bin/env python3
"""
Generate polished CSV from ProductHunt posts NDJSON.
Resolves URLs and handles rate limiting with resume capability.
"""

import asyncio
import csv
import sys
from pathlib import Path
from typing import Dict, Any, Set, Optional
import time

import click
import httpx
import orjson
import pandas as pd
from tqdm.asyncio import tqdm


class URLResolver:
    def __init__(self, max_requests_per_second: int = 15):
        self.max_requests_per_second = max_requests_per_second
        self.request_times = []
        self.client = None
        
    async def __aenter__(self):
        self.client = httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(connect=5.0, read=10.0, write=5.0, pool=5.0),
            headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()
    
    async def _rate_limit(self):
        """Ensure we don't exceed rate limits."""
        now = time.time()
        # Remove requests older than 1 second
        self.request_times = [t for t in self.request_times if now - t < 1.0]
        
        if len(self.request_times) >= self.max_requests_per_second:
            sleep_time = 1.0 - (now - self.request_times[0])
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
        
        self.request_times.append(now)
    
    async def resolve_url(self, ph_url: str, max_retries: int = 2) -> Optional[str]:
        """Resolve ProductHunt shortened URL to final destination."""
        full_url = f"https://producthunt.com{ph_url}"
        
        for attempt in range(max_retries):
            try:
                await self._rate_limit()
                
                response = await self.client.head(full_url)
                
                # Check if we successfully followed redirects to a different domain
                final_url = str(response.url)
                if response.status_code == 200 and not final_url.startswith('https://producthunt.com'):
                    return final_url
                elif response.status_code == 429:
                    # Rate limited, exponential backoff
                    wait_time = (2 ** attempt) * 5
                    print(f"Rate limited, waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                elif response.status_code == 200 and final_url.startswith('https://producthunt.com'):
                    # Still on ProductHunt domain, might need a GET request
                    response = await self.client.get(full_url)
                    final_url = str(response.url)
                    if not final_url.startswith('https://producthunt.com'):
                        return final_url
                    
            except Exception as e:
                error_str = str(e)
                # Fast fail for DNS errors - don't retry
                if any(dns_error in error_str.lower() for dns_error in 
                       ['name resolution', 'name or service not known', 'nodename nor servname']):
                    print(f"DNS error for {ph_url}: {e}")
                    return None
                    
                if attempt == max_retries - 1:
                    print(f"Error resolving {ph_url}: {e}")
                    return None
                await asyncio.sleep(0.5)  # Shorter retry delay
        
        print(f"Failed to resolve URL after {max_retries} attempts: {ph_url}")
        return None


def load_existing_ids(csv_path: Path) -> Set[str]:
    """Load existing ProductHunt IDs from CSV file."""
    if not csv_path.exists():
        return set()
    
    try:
        df = pd.read_csv(csv_path)
        return set(df['ph_id'].astype(str))
    except Exception:
        return set()


def has_required_fields(post: Dict[str, Any]) -> bool:
    """Check if post has all required fields."""
    required_fields = ['id', 'name', 'tagline', 'createdAt', 'shortenedUrl']
    return all(post.get(field) for field in required_fields)


async def process_posts(
    input_file: Path,
    output_file: Path,
    resolver: URLResolver
) -> None:
    """Process posts from NDJSON and write to CSV."""
    
    # Load existing IDs to avoid duplicates
    existing_ids = load_existing_ids(output_file)
    print(f"Found {len(existing_ids)} existing entries in CSV")
    
    # Count total posts for progress bar
    total_posts = 0
    with open(input_file, 'r') as f:
        for line in f:
            if line.strip():
                total_posts += 1
    
    processed = 0
    skipped_incomplete = 0
    skipped_duplicate = 0
    skipped_url_failed = 0
    url_resolved = 0
    
    # Open CSV file for appending
    csv_exists = output_file.exists()
    
    with open(output_file, 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['ph_id', 'name', 'tagline', 'createdAt', 'url']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write header if file is new
        if not csv_exists:
            writer.writeheader()
        
        # Process posts with progress bar
        with tqdm(total=total_posts, desc="Processing posts") as pbar:
            with open(input_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        post = orjson.loads(line)
                    except Exception:
                        pbar.update(1)
                        continue
                    
                    # Check if has required fields
                    if not has_required_fields(post):
                        skipped_incomplete += 1
                        pbar.set_postfix({
                            "Processed": processed,
                            "URL resolved": url_resolved,
                            "Skipped incomplete": skipped_incomplete,
                            "Skipped duplicate": skipped_duplicate,
                            "Skipped URL failed": skipped_url_failed
                        })
                        pbar.update(1)
                        continue
                    
                    ph_id = str(post['id'])
                    
                    # Skip if already processed
                    if ph_id in existing_ids:
                        skipped_duplicate += 1
                        pbar.set_postfix({
                            "Processed": processed,
                            "URL resolved": url_resolved,
                            "Skipped incomplete": skipped_incomplete,
                            "Skipped duplicate": skipped_duplicate,
                            "Skipped URL failed": skipped_url_failed
                        })
                        pbar.update(1)
                        continue
                    
                    # Resolve URL
                    url = await resolver.resolve_url(post['shortenedUrl'])
                    if not url:
                        # Skip post if URL resolution failed
                        skipped_url_failed += 1
                        pbar.set_postfix({
                            "Processed": processed,
                            "URL resolved": url_resolved,
                            "Skipped incomplete": skipped_incomplete,
                            "Skipped duplicate": skipped_duplicate,
                            "Skipped URL failed": skipped_url_failed
                        })
                        pbar.update(1)
                        continue
                    
                    url_resolved += 1
                    
                    # Write to CSV
                    row = {
                        'ph_id': ph_id,
                        'name': post['name'],
                        'tagline': post['tagline'],
                        'createdAt': post['createdAt'],
                        'url': url
                    }
                    
                    writer.writerow(row)
                    csvfile.flush()  # Ensure data is written immediately
                    
                    existing_ids.add(ph_id)
                    processed += 1
                    
                    pbar.set_postfix({
                        "Processed": processed,
                        "URL resolved": url_resolved,
                        "Skipped incomplete": skipped_incomplete,
                        "Skipped duplicate": skipped_duplicate,
                        "Skipped URL failed": skipped_url_failed
                    })
                    pbar.update(1)
    
    print(f"\nCompleted!")
    print(f"Processed: {processed}")
    print(f"URLs resolved: {url_resolved}")
    print(f"Skipped (incomplete): {skipped_incomplete}")
    print(f"Skipped (duplicate): {skipped_duplicate}")
    print(f"Skipped (URL failed): {skipped_url_failed}")


@click.command()
@click.option('--input', '-i', default='posts.ndjson', help='Input NDJSON file')
@click.option('--output', '-o', default='posts.csv', help='Output CSV file')
@click.option('--rate-limit', '-r', default=15, help='Max requests per second')
def main(input: str, output: str, rate_limit: int):
    """
    Convert ProductHunt posts NDJSON to polished CSV with URL resolution.
    """
    input_path = Path(input)
    output_path = Path(output)
    
    if not input_path.exists():
        click.echo(f"Error: Input file {input_path} not found", err=True)
        return
    
    click.echo(f"Processing {input_path} -> {output_path}")
    click.echo(f"Rate limit: {rate_limit} requests/second")
    
    async def run():
        async with URLResolver(max_requests_per_second=rate_limit) as resolver:
            await process_posts(input_path, output_path, resolver)
    
    asyncio.run(run())


if __name__ == '__main__':
    main() 