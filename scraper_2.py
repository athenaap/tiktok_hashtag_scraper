"""
TikTok Hashtag Scraper with Comment Support
Scrapes videos from TikTok by hashtag (e.g., #adauniversity)
"""

import asyncio
import json
import re
from typing import List, Dict, Optional
from datetime import datetime
from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeout
import argparse


class TikTokHashtagScraper:
    """Scraper for TikTok hashtag pages"""
    
    def __init__(self, headless: bool = True, proxy: Optional[str] = None):
        self.headless = headless
        self.proxy = proxy
        self.base_url = "https://www.tiktok.com/tag/{}"
        
    async def scrape_hashtag(
        self, 
        hashtag: str, 
        max_videos: int = 30,
        scroll_pause: float = 2.0,
        detailed_scrape: bool = False,
        video_delay: float = 1.5,
        scrape_comments: bool = False,
        max_comments: int = 20
    ) -> Dict:
        """
        Scrape videos from a TikTok hashtag
        
        Args:
            hashtag: The hashtag to scrape (with or without #)
            max_videos: Maximum number of videos to scrape
            scroll_pause: Time to wait between scrolls (seconds)
            detailed_scrape: If True, scrape each video page for detailed stats
            video_delay: Delay between video page requests (seconds)
            scrape_comments: If True, scrape comments from each video
            max_comments: Maximum comments to scrape per video
            
        Returns:
            Dictionary with hashtag info and list of videos
        """
        # Clean hashtag (remove # if present)
        hashtag = hashtag.lstrip('#')
        url = self.base_url.format(hashtag)
        
        print(f"🔍 Scraping hashtag: #{hashtag}")
        print(f"📍 URL: {url}")
        if detailed_scrape:
            print(f"🔬 Detailed scraping enabled - will visit each video page")
        if scrape_comments:
            print(f"💬 Comment scraping enabled - will scrape up to {max_comments} comments per video")
        
        async with async_playwright() as p:
            # Launch browser with anti-detection settings
            browser_args = {
                'headless': self.headless,
                'args': [
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                ]
            }
            
            if self.proxy:
                browser_args['proxy'] = {'server': self.proxy}
            
            browser = await p.chromium.launch(**browser_args)
            
            # Create context with realistic settings
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
                timezone_id='America/New_York'
            )
            
            page = await context.new_page()
            print(f"🌐 Browser opened: {page}")

            
            try:
                # Navigate to hashtag page
                await page.goto(url, wait_until='domcontentloaded', timeout=30000)
                print(f"🌐 Navigated to hashtag page: {url}")

                await page.get_by_role("button", name="Refresh").click()



                
                # Wait for content to load
                await asyncio.sleep(3)
                
                # Extract hashtag info and videos
                hashtag_data = await self._extract_hashtag_data(
                    page, hashtag, max_videos, scroll_pause, 
                    context if detailed_scrape else None, 
                    video_delay,
                    scrape_comments,
                    max_comments
                )
                
                print(f"✅ Successfully scraped {len(hashtag_data.get('videos', []))} videos")
                
                return hashtag_data
                
            except PlaywrightTimeout:
                print(f"⚠️ Timeout while loading page. TikTok may be blocking requests.")
                return {'hashtag': hashtag, 'videos': [], 'error': 'timeout'}
                
            except Exception as e:
                print(f"❌ Error: {str(e)}")
                return {'hashtag': hashtag, 'videos': [], 'error': str(e)}
                
            finally:
                await browser.close()

    
    async def _extract_hashtag_data(
        self, 
        page: Page, 
        hashtag: str, 
        max_videos: int,
        scroll_pause: float,
        context = None,
        video_delay: float = 1.5,
        scrape_comments: bool = False,
        max_comments: int = 20
    ) -> Dict:
        """Extract hashtag information and video data from the page"""
        
        # Try to extract from JSON data first
        hashtag_info = await self._extract_from_json(page, hashtag)
        
        # If JSON extraction fails, try scrolling and extracting from HTML
        if not hashtag_info.get('videos'):
            print("📜 JSON extraction incomplete, attempting HTML scraping with scrolling...")
            videos = await self._scrape_videos_by_scrolling(
                page, max_videos, scroll_pause, context, video_delay, 
                scrape_comments, max_comments
            )
            hashtag_info['videos'] = videos
        
        return hashtag_info
    
    async def _extract_from_json(self, page: Page, hashtag: str) -> Dict:
        """Extract data from embedded JSON in the page"""
        
        try:
            # TikTok embeds data in script tags
            script_data = await page.evaluate("""
                () => {
                    const scripts = document.querySelectorAll('script');
                    for (let script of scripts) {
                        if (script.id === '__UNIVERSAL_DATA_FOR_REHYDRATION__' || 
                            script.id === 'SIGI_STATE') {
                            try {
                                return script.textContent;
                            } catch (e) {
                                continue;
                            }
                        }
                    }
                    return null;
                }
            """)
            
            if not script_data:
                print("⚠️ Could not find embedded JSON data")
                return {'hashtag': hashtag, 'videos': []}
            
            # Parse JSON
            data = json.loads(script_data)
            
            # Extract hashtag info and videos
            hashtag_info = self._parse_hashtag_json(data, hashtag)
            
            return hashtag_info
            
        except Exception as e:
            print(f"⚠️ JSON extraction error: {str(e)}")
            return {'hashtag': hashtag, 'videos': []}
    
    def _parse_hashtag_json(self, data: Dict, hashtag: str) -> Dict:
        """Parse hashtag and video data from JSON object"""
        
        result = {
            'hashtag': hashtag,
            'scraped_at': datetime.now().isoformat(),
            'hashtag_info': {},
            'videos': []
        }
        
        try:
            # Try to find hashtag challenge info
            if '__DEFAULT_SCOPE__' in data:
                scope = data['__DEFAULT_SCOPE__']
                
                # Look for challenge info
                if 'webapp.challenge-detail' in scope:
                    challenge = scope['webapp.challenge-detail']
                    if 'challengeInfo' in challenge:
                        info = challenge['challengeInfo']['challenge']
                        result['hashtag_info'] = {
                            'id': info.get('id'),
                            'title': info.get('title'),
                            'description': info.get('desc'),
                            'view_count': info.get('viewCount'),
                            'video_count': info.get('videoCount'),
                            'is_commerce': info.get('isCommerce', False)
                        }
                
                # Look for video items
                for key in scope:
                    if 'itemList' in str(key) or 'itemModule' in str(key):
                        items = scope[key]
                        if isinstance(items, dict):
                            for item_id, item_data in items.items():
                                if isinstance(item_data, dict) and 'video' in item_data:
                                    video = self._parse_video_item(item_data)
                                    if video:
                                        result['videos'].append(video)
        
        except Exception as e:
            print(f"⚠️ Error parsing JSON: {str(e)}")
        
        return result
    
    def _parse_video_item(self, item: Dict) -> Optional[Dict]:
        """Parse individual video item from JSON"""
        
        try:
            video_data = {
                'id': item.get('id'),
                'description': item.get('desc', ''),
                'created_at': item.get('createTime'),
                'author': {
                    'id': item.get('author', {}).get('id'),
                    'username': item.get('author', {}).get('uniqueId'),
                    'nickname': item.get('author', {}).get('nickname'),
                    'verified': item.get('author', {}).get('verified', False),
                    'avatar': item.get('author', {}).get('avatarThumb')
                },
                'stats': {
                    'views': item.get('stats', {}).get('playCount', 0),
                    'likes': item.get('stats', {}).get('diggCount', 0),
                    'comments': item.get('stats', {}).get('commentCount', 0),
                    'shares': item.get('stats', {}).get('shareCount', 0)
                },
                'video': {
                    'duration': item.get('video', {}).get('duration'),
                    'ratio': item.get('video', {}).get('ratio'),
                    'cover': item.get('video', {}).get('cover'),
                    'download_url': item.get('video', {}).get('downloadAddr'),
                    'play_url': item.get('video', {}).get('playAddr')
                },
                'music': {
                    'id': item.get('music', {}).get('id'),
                    'title': item.get('music', {}).get('title'),
                    'author': item.get('music', {}).get('authorName')
                },
                'hashtags': [tag.get('title') for tag in item.get('challenges', [])],
                'url': f"https://www.tiktok.com/@{item.get('author', {}).get('uniqueId')}/video/{item.get('id')}"
            }
            
            return video_data
            
        except Exception as e:
            print(f"⚠️ Error parsing video item: {str(e)}")
            return None
    
    async def _scrape_videos_by_scrolling(
        self, 
        page: Page, 
        max_videos: int,
        scroll_pause: float,
        context = None,
        video_delay: float = 1.5,
        scrape_comments: bool = False,
        max_comments: int = 20
    ) -> List[Dict]:
        """Scrape videos by scrolling the page and extracting from HTML"""
        
        videos = []
        last_height = 0
        scroll_attempts = 0
        max_scroll_attempts = 20
        
        print(f"🔄 Starting to scroll and collect videos (target: {max_videos})...")
        
        while len(videos) < max_videos and scroll_attempts < max_scroll_attempts:

            # Extract video links from current view
            video_elements = await page.query_selector_all('[data-e2e="challenge-item"]')
            
            for element in video_elements:
                if len(videos) >= max_videos:
                    break
                
                try:
                    # Extract video URL
                    link = await element.query_selector('a')
                    if link:
                        href = await link.get_attribute('href')
                        if href and href not in [v.get('url') for v in videos]:
                            # Extract basic info from visible elements
                            video_info = await self._extract_video_info_from_element(element, href)
                            if video_info:
                                videos.append(video_info)
                                print(f"  📹 Collected video {len(videos)}/{max_videos}")
                
                except Exception as e:
                    continue
            
            # Scroll down
            current_height = await page.evaluate('document.body.scrollHeight')
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            await asyncio.sleep(scroll_pause)
            
            new_height = await page.evaluate('document.body.scrollHeight')
            
            # Check if we've reached the bottom
            if new_height == current_height:
                scroll_attempts += 1
            else:
                scroll_attempts = 0
            
            last_height = new_height
        
        # If detailed scraping is enabled, visit each video page
        if context:
            print(f"\n🔬 Starting detailed scraping for {len(videos)} videos...")
            detailed_videos = []
            for i, video in enumerate(videos, 1):
                print(f"  📹 Scraping details for video {i}/{len(videos)}: {video.get('url')}")
                detailed_info = await self.scrape_video_details(
                    video['url'], context, video_delay, scrape_comments, max_comments
                )
                if detailed_info:
                    detailed_videos.append(detailed_info)
                else:
                    # If detailed scraping fails, keep the basic info
                    detailed_videos.append(video)
            return detailed_videos[:max_videos]
        
        return videos[:max_videos]
    
    async def _extract_video_info_from_element(self, element, url: str) -> Optional[Dict]:
        """Extract video information from HTML element"""
        
        try:
            # Extract video ID from URL
            video_id = re.search(r'/video/(\d+)', url)
            video_id = video_id.group(1) if video_id else None
            
            # Try to extract visible stats
            stats_text = await element.inner_text()
            
            return {
                'id': video_id,
                'url': url if url.startswith('http') else f"https://www.tiktok.com{url}",
                'stats_text': stats_text,
                'scraped_via': 'html_scroll',
                'stats': {
                    'views': 0,
                    'likes': 0,
                    'comments': 0,
                    'shares': 0
                }
            }
            
        except Exception as e:
            return None
    
    async def scrape_video_details(
        self, 
        video_url: str, 
        context,
        delay: float = 1.5,
        scrape_comments: bool = False,
        max_comments: int = 20
    ) -> Optional[Dict]:
        """
        Scrape detailed information from an individual video page
        
        Args:
            video_url: URL of the video to scrape
            context: Browser context to use
            delay: Delay before scraping (seconds)
            scrape_comments: Whether to scrape comments
            max_comments: Maximum number of comments to scrape
            
        Returns:
            Dictionary with detailed video information
        """
        try:
            # Add random jitter to delay
            import random
            actual_delay = delay + random.uniform(0, 0.5)
            await asyncio.sleep(actual_delay)
            
            page = await context.new_page()
            
            try:
                # Navigate to video page
                await page.goto(video_url, wait_until='domcontentloaded', timeout=30000)
                await asyncio.sleep(2)
                
                # Extract JSON data from the page
                script_data = await page.evaluate("""
                    () => {
                        const scripts = document.querySelectorAll('script');
                        for (let script of scripts) {
                            if (script.id === '__UNIVERSAL_DATA_FOR_REHYDRATION__' || 
                                script.id === 'SIGI_STATE') {
                                try {
                                    return script.textContent;
                                } catch (e) {
                                    continue;
                                }
                            }
                        }
                        return null;
                    }
                """)
                
                if not script_data:
                    print(f"  ⚠️ No JSON data found for {video_url}")
                    return None
                
                # Parse the JSON data
                data = json.loads(script_data)
                video_details = self._parse_video_details_json(data, video_url)
                
                # Scrape comments if requested
                if scrape_comments and video_details:
                    comment_count = video_details.get('stats', {}).get('comments', 0)
                    if comment_count > 0:
                        print(f"    💬 Scraping comments (video has {comment_count} comments)...")
                        comments = await self._scrape_comments_from_page(page, max_comments)
                        if comments:
                            video_details['comments'] = comments
                            print(f"    ✅ Scraped {len(comments)} comments")
                
                return video_details
                
            finally:
                await page.close()
                
        except Exception as e:
            print(f"  ⚠️ Error scraping video details: {str(e)}")
            return None
    
    async def _scrape_comments_from_page(self, page: Page, max_comments: int = 20) -> List[Dict]:
        """
        Scrape comments from a video page by scrolling the comment section
        
        Args:
            page: The page object
            max_comments: Maximum number of comments to scrape
            
        Returns:
            List of comment dictionaries
        """
        comments = []
        
        try:
            # Wait for comments to load
            await asyncio.sleep(2)
            
            # Try to find and click "View more comments" or scroll comment section
            # Different selectors for comment containers
            comment_selectors = [
                '[data-e2e="comment-level-1"]',
                # '.tiktok-comment',
                # '[class*="DivCommentItemContainer"]',
                # '[class*="CommentItem"]'
            ]
            
            
            icon = await page.query_selector('[data-e2e="comment-icon"]')
            await icon.click()  # 🖱️ Click to load comments!
            await asyncio.sleep(3)  # Wait for AJAX
            comments = await page.query_selector_all('[data-e2e="comment-level-1"]')
            
            if not comments:
                print(f"    ⚠️ No comment elements found")
                return []
            
            # Scroll within comment section to load more comments
            scroll_attempts = 0
            max_scroll = 5
            
            while len(comments) < max_comments and scroll_attempts < max_scroll:
                # Try to scroll the comment section
                try:
                    await page.evaluate("""
                        () => {
                            const commentSection = document.querySelector('[data-e2e="comment-list"]') || 
                                                  document.querySelector('[class*="CommentList"]');
                            if (commentSection) {
                                commentSection.scrollTop = commentSection.scrollHeight;
                            } else {
                                window.scrollTo(0, document.body.scrollHeight);
                            }
                        }
                    """)
                    await asyncio.sleep(1.5)
                    
                    # Re-query for comments
                    for selector in comment_selectors:
                        try:
                            elements = await page.query_selector_all(selector)
                            if len(elements) > len(comments):
                                comments = elements
                                break
                        except:
                            continue
                    
                except:
                    pass
                
                scroll_attempts += 1
            
            # Extract comment data from elements
            for i, element in enumerate(comments[:max_comments]):
                try:
                    comment_data = await self._extract_comment_data(element, page)
                    if comment_data:
                        comments.append(comment_data)
                except Exception as e:
                    print(f"    ⚠️ Error extracting comment {i+1}: {str(e)}")
                    continue
            
        except Exception as e:
            print(f"    ⚠️ Error scraping comments: {str(e)}")
        
        return comments
    
    async def _extract_comment_data(self, element, page: Page) -> Optional[Dict]:
        """Extract data from a single comment element"""
        
        try:
            # Extract comment text
            text_selectors = [
                '[data-e2e="comment-text"]',
                '[class*="CommentText"]',
                'span[class*="SpanText"]'
            ]
            
            comment_text = ""
            for selector in text_selectors:
                try:
                    text_elem = await element.query_selector(selector)
                    if text_elem:
                        comment_text = await text_elem.inner_text()
                        break
                except:
                    continue
            
            # If we still don't have text, try getting all text from element
            if not comment_text:
                comment_text = await element.inner_text()
            
            # Extract username
            username_selectors = [
                '[data-e2e="comment-username"]',
                '[class*="CommentUsername"]',
                'a[class*="StyledUserLinkName"]'
            ]
            
            username = ""
            for selector in username_selectors:
                try:
                    user_elem = await element.query_selector(selector)
                    if user_elem:
                        username = await user_elem.inner_text()
                        break
                except:
                    continue
            
            # Extract like count
            like_selectors = [
                '[data-e2e="comment-like-count"]',
                '[class*="LikeCount"]'
            ]
            
            likes = 0
            for selector in like_selectors:
                try:
                    like_elem = await element.query_selector(selector)
                    if like_elem:
                        like_text = await like_elem.inner_text()
                        # Parse like count (handle K, M suffixes)
                        likes = self._parse_count(like_text)
                        break
                except:
                    continue
            
            # Extract timestamp
            time_selectors = [
                '[data-e2e="comment-time"]',
                'span[class*="CommentTime"]',
                'time'
            ]
            
            timestamp = ""
            for selector in time_selectors:
                try:
                    time_elem = await element.query_selector(selector)
                    if time_elem:
                        timestamp = await time_elem.inner_text()
                        break
                except:
                    continue
            
            if comment_text:  # Only return if we got the text
                return {
                    'text': comment_text.strip(),
                    'author': username.strip() if username else 'Unknown',
                    'likes': likes,
                    'timestamp': timestamp.strip() if timestamp else ''
                }
            
            return None
            
        except Exception as e:
            return None
    
    def _parse_count(self, count_str: str) -> int:
        """Parse count strings like '1.2K', '3M' to integers"""
        try:
            count_str = count_str.strip().upper()
            if 'K' in count_str:
                return int(float(count_str.replace('K', '')) * 1000)
            elif 'M' in count_str:
                return int(float(count_str.replace('M', '')) * 1000000)
            else:
                return int(count_str)
        except:
            return 0
    
    def _parse_video_details_json(self, data: Dict, video_url: str) -> Optional[Dict]:
        """Parse detailed video information from JSON data"""
        
        try:
            video_info = {
                'url': video_url,
                'scraped_via': 'video_page'
            }
            
            # Navigate through the JSON structure to find video data
            if '__DEFAULT_SCOPE__' in data:
                scope = data['__DEFAULT_SCOPE__']
                
                # Look for video detail data
                if 'webapp.video-detail' in scope:
                    detail = scope['webapp.video-detail']
                    
                    if 'itemInfo' in detail and 'itemStruct' in detail['itemInfo']:
                        item = detail['itemInfo']['itemStruct']
                        
                        # Extract video ID and description
                        video_info['id'] = item.get('id')
                        video_info['description'] = item.get('desc', '')
                        video_info['created_at'] = item.get('createTime')
                        
                        # Extract author information
                        if 'author' in item:
                            author = item['author']
                            video_info['author'] = {
                                'id': author.get('id'),
                                'username': author.get('uniqueId'),
                                'nickname': author.get('nickname'),
                                'verified': author.get('verified', False),
                                'avatar': author.get('avatarThumb'),
                                'signature': author.get('signature', '')
                            }
                        
                        # Extract stats
                        if 'stats' in item:
                            stats = item['stats']
                            video_info['stats'] = {
                                'views': stats.get('playCount', 0),
                                'likes': stats.get('diggCount', 0),
                                'comments': stats.get('commentCount', 0),
                                'shares': stats.get('shareCount', 0),
                                'saves': stats.get('collectCount', 0)
                            }
                        
                        # Extract video details
                        if 'video' in item:
                            video = item['video']
                            video_info['video'] = {
                                'duration': video.get('duration'),
                                'ratio': video.get('ratio'),
                                'cover': video.get('cover'),
                                'download_url': video.get('downloadAddr'),
                                'play_url': video.get('playAddr'),
                                'width': video.get('width'),
                                'height': video.get('height')
                            }
                        
                        # Extract music information
                        if 'music' in item:
                            music = item['music']
                            video_info['music'] = {
                                'id': music.get('id'),
                                'title': music.get('title'),
                                'author': music.get('authorName'),
                                'duration': music.get('duration'),
                                'original': music.get('original', False)
                            }
                        
                        # Extract hashtags
                        if 'challenges' in item:
                            video_info['hashtags'] = [
                                {
                                    'id': tag.get('id'),
                                    'title': tag.get('title'),
                                    'description': tag.get('desc', '')
                                }
                                for tag in item.get('challenges', [])
                            ]
                        
                        return video_info
            
            return None
            
        except Exception as e:
            print(f"  ⚠️ Error parsing video details JSON: {str(e)}")
            return None



async def main():
    """Main function to run the scraper"""
    
    parser = argparse.ArgumentParser(description='TikTok Hashtag Scraper')
    parser.add_argument('hashtag', help='Hashtag to scrape (with or without #)')
    parser.add_argument('--max-videos', type=int, default=30, help='Maximum number of videos to scrape')
    parser.add_argument('--output', default='tiktok_hashtag_data.json', help='Output JSON file')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    parser.add_argument('--proxy', help='Proxy server (e.g., http://proxy:port)')
    parser.add_argument('--scroll-pause', type=float, default=2.0, help='Pause between scrolls (seconds)')
    parser.add_argument('--detailed', action='store_true', help='Enable detailed scraping (visits each video page)')
    parser.add_argument('--video-delay', type=float, default=1.5, help='Delay between video page requests (seconds)')
    parser.add_argument('--scrape-comments', action='store_true', help='Scrape comments from videos')
    parser.add_argument('--max-comments', type=int, default=20, help='Maximum comments to scrape per video')
    
    args = parser.parse_args()
    
    # Create scraper
    scraper = TikTokHashtagScraper(headless=args.headless, proxy=args.proxy)
    
    # Scrape hashtag
    data = await scraper.scrape_hashtag(
        hashtag=args.hashtag,
        max_videos=args.max_videos,
        scroll_pause=args.scroll_pause,
        detailed_scrape=args.detailed,
        video_delay=args.video_delay,
        scrape_comments=args.scrape_comments,
        max_comments=args.max_comments
    )
    def clean_for_json(obj):
        """Recursively clean data to ensure JSON serializability"""
        if isinstance(obj, dict):
            return {k: clean_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [clean_for_json(item) for item in obj]
        elif isinstance(obj, (str, int, float, bool, type(None))):
            return obj
        else:
            # Convert anything else to string
            return str(obj)

    data = clean_for_json(data)

    
    # Save to file
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Data saved to: {args.output}")
    print(f"📊 Total videos scraped: {len(data.get('videos', []))}")
    
    # Print summary
    if data.get('hashtag_info'):
        info = data['hashtag_info']
        print("\n📈 Hashtag Stats:")
        print(f"  Views: {info.get('view_count', 'N/A')}")
        print(f"  Videos: {info.get('video_count', 'N/A')}")
    
    # Print comment stats if scraped
    if args.scrape_comments:
        total_comments = sum(len(v.get('comments', [])) for v in data.get('videos', []))
        videos_with_comments = sum(1 for v in data.get('videos', []) if v.get('comments'))
        print(f"\n💬 Comment Stats:")
        print(f"  Total comments scraped: {total_comments}")
        print(f"  Videos with comments: {videos_with_comments}/{len(data.get('videos', []))}")


if __name__ == "__main__":
    asyncio.run(main())
