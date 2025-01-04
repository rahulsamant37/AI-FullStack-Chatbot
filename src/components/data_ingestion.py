import asyncio
from playwright.async_api import async_playwright
from datetime import datetime
import random
import os
import json
from src import logger
from typing import List, Dict, Any, Optional
from src.entity.config_entity import (DataIngestionConfig)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
]

class DataIngestionError(Exception):
    """Custom exception for data ingestion errors"""
    pass

class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config
        self.companies: List[Dict[str, Any]] = []
        self.logs_dir = os.path.join(self.config.root_dir, "logs")
        self.output_dir = os.path.join(self.config.root_dir, "raw")
        self.semaphore = asyncio.Semaphore(4)
        self.logger = logger
        
        os.makedirs(self.output_dir, exist_ok=True)

    async def initiate_data_ingestion(self) -> Optional[str]:
        try:
            # Initialize playwright
            self.logger.info("Initializing playwright...")
            playwright = await async_playwright().start()
            if not playwright:
                raise DataIngestionError("Failed to initialize playwright")
            
            self.logger.info("Launching browser...")
            browser = await playwright.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu'
                ]
            )
            
            if not browser:
                raise DataIngestionError("Failed to launch browser")
            self.logger.info("Browser launched successfully")

            try:
                self.logger.info("Creating browser context...")
                context = await browser.new_context(
                    user_agent=random.choice(USER_AGENTS),
                    viewport={'width': 1920, 'height': 1080},
                    ignore_https_errors=True
                )
                
                if not context:
                    raise DataIngestionError("Failed to create browser context")
                self.logger.info("Browser context created successfully")

                try:
                    self.logger.info("Creating new page...")
                    page = await context.new_page()
                    
                    if not page:
                        raise DataIngestionError("Failed to create new page")
                    self.logger.info("New page created successfully")

                    # Set timeout after verifying page exists
                    self.logger.info("Setting page timeout...")
                    page.set_default_timeout(30000)  # Removed await as it's not an async method
                    
                    self.logger.info("Starting company listings scrape...")
                    await self.scrape_companies(page)
                    
                    if not self.companies:
                        raise DataIngestionError("No companies found during scraping")
                    
                    self.logger.info(f"Found {len(self.companies)} companies")
                    self.logger.info("Starting parallel job scraping...")
                    
                    tasks = [self.process_company(browser, company) 
                            for company in self.companies]
                    
                    await asyncio.gather(*tasks)
                    
                finally:
                    self.logger.info("Closing page...")
                    if page:
                        await page.close()
            
            finally:
                self.logger.info("Closing context...")
                if context:
                    await context.close()
                
                self.logger.info("Closing browser...")
                if browser:
                    await browser.close()
                
                self.logger.info("Closing playwright...")
                if playwright:
                    await playwright.stop()

            self.logger.info("Data ingestion completed successfully")
            return self.output_dir

        except Exception as e:
            self.logger.error(f"Data ingestion failed: {str(e)}")
            raise DataIngestionError(f"Data ingestion failed: {str(e)}") from e

    async def scrape_companies(self, page) -> None:
        try:
            self.logger.info(f"Navigating to {self.config.source_URL}")
            await page.goto(self.config.source_URL, wait_until='networkidle')
            self.logger.info("Waiting for company selector...")
            await page.wait_for_selector('.row li.clicky')
            
            companies = await page.query_selector_all('.row li.clicky')
            self.logger.info(f"Found {len(companies)} companies on the page")
            
            for company in companies:
                try:
                    name = await (await company.query_selector('strong')).inner_text()
                    job_count = (await (await company.query_selector('.jobs-count')).inner_text()).split()[0]
                    url = await company.get_attribute('data-url')
                    
                    self.companies.append({
                        'name': name.strip(),
                        'job_count': int(job_count),
                        'url': f"https://www.careerjet.co.in{url}"
                    })
                except Exception as e:
                    self.logger.error(f"Error processing company element: {str(e)}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Company scraping failed: {str(e)}")
            raise DataIngestionError(f"Company scraping failed: {str(e)}") from e

    async def process_company(self, browser, company: Dict[str, Any]) -> None:
        async with self.semaphore:
            company_name = company.get('name', 'Unknown Company')
            self.logger.info(f"Starting job scrape for {company_name}")
            
            try:
                context = await browser.new_context(
                    user_agent=random.choice(USER_AGENTS),
                    viewport={'width': 1920, 'height': 1080},
                    ignore_https_errors=True
                )
                
                if not context:
                    raise DataIngestionError(f"Failed to create context for {company_name}")
                
                page = await context.new_page()
                if not page:
                    raise DataIngestionError(f"Failed to create page for {company_name}")
                
                page.set_default_timeout(30000)  # Removed await as it's not an async method
                
                jobs = await self.scrape_company_jobs(page, company)
                
                if jobs:
                    await self.save_company_data(company, jobs)
                else:
                    self.logger.warning(f"No jobs found for {company_name}")
                
                await context.close()
                
            except Exception as e:
                self.logger.error(f"Error processing company {company_name}: {str(e)}")

    async def scrape_company_jobs(self, page, company: Dict[str, Any]) -> List[Dict[str, Any]]:
        company_jobs = []
        max_pages = 4

        try:
            for page_num in range(1, max_pages + 1):
                if page_num == 1:
                    await page.goto(company['url'])
                else:
                    next_button = await page.query_selector('button.ves-control.next')
                    if not next_button:
                        break
                    await next_button.click()
                
                try:
                    await page.wait_for_selector('article.job', timeout=10000)
                    await asyncio.sleep(2)
                    
                    jobs = await page.query_selector_all('article.job')
                    if not jobs:
                        break
                    
                    for job in jobs:
                        try:
                            job_data = await self.extract_job_data(job)
                            company_jobs.append(job_data)
                        except Exception as e:
                            self.logger.error(f"Error extracting job data: {str(e)}")
                            continue
                    
                    await asyncio.sleep(2)
                except Exception as e:
                    self.logger.error(f"Error on page {page_num}: {str(e)}")
                    break
                    
        except Exception as e:
            self.logger.error(f"Error scraping jobs: {str(e)}")
        
        return company_jobs

    async def extract_job_data(self, job) -> Dict[str, Any]:
        title = await (await job.query_selector('h2 a')).inner_text()
        
        location_elem = await job.query_selector('.location li')
        location = await location_elem.inner_text() if location_elem else "Not specified"
        
        desc_elem = await job.query_selector('.desc')
        description = await desc_elem.inner_text() if desc_elem else "No description available"
        
        posted_elem = await job.query_selector('.tags .badge')
        posted = await posted_elem.inner_text() if posted_elem else "Not specified"
        
        return {
            'title': title.strip(),
            'location': location.strip(),
            'description': description.strip(),
            'posted': posted.strip()
        }

    async def save_company_data(self, company: Dict[str, Any], jobs: List[Dict[str, Any]]) -> None:
        try:
            current_date = datetime.now().strftime("%Y-%m-%d")
            company_name_safe = "".join(x for x in company['name'] if x.isalnum() or x in (' ', '-', '_')).strip()
            filename = os.path.join(self.output_dir, f"{company_name_safe}_jobs.json")

            json_data = {
                "company_name": company['name'],
                "scraped_on": current_date,
                "total_jobs_available": company['job_count'],
                "company_url": company['url'],
                "jobs": jobs
            }

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=4)

            self.logger.info(f"Generated JSON file: {filename}")
        except Exception as e:
            self.logger.error(f"Error saving data for {company['name']}: {str(e)}")