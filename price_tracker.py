"""
Amazon Price Tracker & Alert System
====================================
Async web scraper monitoring Amazon prices with SQLite storage,
email alerts, CSV export, and retry logic.
Author: Joseph Davis
"""
import asyncio, csv, hashlib, logging, random, re, sqlite3, smtplib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from typing import Optional
import aiohttp
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("price_tracker")
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) Chrome/119.0",
    "Mozilla/5.0 (X11; Linux x86_64) Chrome/118.0",
]

@dataclass
class Product:
    url: str
    name: str = ""
    current_price: Optional[float] = None
    target_price: Optional[float] = None
    currency: str = "USD"
    product_id: str = field(default="", init=False)
    def __post_init__(self):
        self.product_id = hashlib.md5(self.url.encode()).hexdigest()[:12]

@dataclass
class PriceRecord:
    product_id: str
    price: float
    currency: str
    timestamp: datetime

class PriceDatabase:
    """SQLite storage for products and price history."""
    def __init__(self, db_path="price_history.db"):
        self.conn = sqlite3.connect(db_path)
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS products (
                product_id TEXT PRIMARY KEY, url TEXT NOT NULL,
                name TEXT, target_price REAL, currency TEXT DEFAULT 'USD',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id TEXT NOT NULL, price REAL NOT NULL,
                currency TEXT DEFAULT 'USD',
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(product_id));
            CREATE INDEX IF NOT EXISTS idx_ph ON price_history(product_id, recorded_at);
        """)
        self.conn.commit()

    def add_product(self, p: Product):
        self.conn.execute("INSERT OR REPLACE INTO products VALUES (?,?,?,?,?)",
            (p.product_id, p.url, p.name, p.target_price, p.currency))
        self.conn.commit()

    def record_price(self, r: PriceRecord):
        self.conn.execute("INSERT INTO price_history (product_id,price,currency,recorded_at) VALUES (?,?,?,?)",
            (r.product_id, r.price, r.currency, r.timestamp))
        self.conn.commit()

    def get_history(self, pid, days=30):
        since = datetime.now() - timedelta(days=days)
        rows = self.conn.execute(
            "SELECT product_id,price,currency,recorded_at FROM price_history WHERE product_id=? AND recorded_at>=? ORDER BY recorded_at",
            (pid, since)).fetchall()
        return [PriceRecord(r[0],r[1],r[2],datetime.fromisoformat(r[3])) for r in rows]

    def get_products(self):
        rows = self.conn.execute("SELECT product_id,url,name,target_price,currency FROM products").fetchall()
        out = []
        for r in rows:
            p = Product(url=r[1], name=r[2], target_price=r[3], currency=r[4])
            p.product_id = r[0]
            out.append(p)
        return out

    def get_lowest(self, pid):
        r = self.conn.execute("SELECT MIN(price) FROM price_history WHERE product_id=?", (pid,)).fetchone()
        return r[0] if r and r[0] else None

class AmazonScraper:
    """Async scraper with rotating UAs and retry."""
    def __init__(self, retries=3, delay=(2,5)):
        self.retries, self.delay, self.session = retries, delay, None
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(); return self
    async def __aexit__(self, *a):
        if self.session: await self.session.close()
    async def fetch(self, url):
        for i in range(1, self.retries+1):
            try:
                await asyncio.sleep(random.uniform(*self.delay))
                h = {"User-Agent": random.choice(USER_AGENTS), "Accept": "text/html"}
                async with self.session.get(url, headers=h, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status == 200: return await r.text()
                    if r.status == 503: await asyncio.sleep(2*i)
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                logger.warning(f"Attempt {i}: {e}")
        return None
    def parse(self, html, url):
        soup = BeautifulSoup(html, "html.parser")
        t = soup.find("span", {"id": "productTitle"})
        name = t.get_text(strip=True) if t else "Unknown"
        for tag, a in [("span",{"class":"a-price-whole"}),("span",{"id":"priceblock_ourprice"}),("span",{"class":"a-offscreen"})]:
            el = soup.find(tag, a)
            if el:
                try:
                    return Product(url=url, name=name[:200], current_price=float(re.sub(r"[^\\d.]","",el.get_text(strip=True))))
                except ValueError: continue
        return None
    async def scrape(self, url):
        html = await self.fetch(url)
        return self.parse(html, url) if html else None

class EmailAlert:
    """SMTP price drop alerts."""
    def __init__(self, host="smtp.gmail.com", port=587, sender="", pw="", to=""):
        self.host, self.port, self.sender, self.pw, self.to = host, port, sender, pw, to
    def send(self, product, lowest):
        msg = MIMEMultipart()
        msg["Subject"] = f"Price Drop: {product.name[:50]}"
        msg["From"], msg["To"] = self.sender, self.to
        body = f"<h2>Price Drop!</h2><p>{product.name}</p><p>${product.current_price:.2f}</p>"
        if lowest: body += f"<p>All-time low: ${lowest:.2f}</p>"
        msg.attach(MIMEText(body, "html"))
        try:
            with smtplib.SMTP(self.host, self.port) as s:
                s.starttls(); s.login(self.sender, self.pw)
                s.sendmail(self.sender, self.to, msg.as_string())
        except Exception as e: logger.error(f"Alert failed: {e}")

class PriceTracker:
    """Orchestrates scraping, storage, alerting, and export."""
    def __init__(self, db, alert=None): self.db, self.alert = db, alert
    def add(self, url, target):
        p = Product(url=url, target_price=target); self.db.add_product(p); return p
    async def check(self):
        prods = self.db.get_products()
        async with AmazonScraper() as s:
            results = await asyncio.gather(*[s.scrape(p.url) for p in prods], return_exceptions=True)
        for prod, res in zip(prods, results):
            if isinstance(res, Exception) or not res: continue
            self.db.record_price(PriceRecord(prod.product_id, res.current_price, res.currency, datetime.now()))
            if prod.target_price and res.current_price <= prod.target_price:
                logger.info(f"PRICE DROP: {prod.name} ${res.current_price:.2f}")
                if self.alert:
                    res.target_price = prod.target_price
                    self.alert.send(res, self.db.get_lowest(prod.product_id))
    def export_csv(self, days=30):
        Path("exports").mkdir(exist_ok=True)
        for p in self.db.get_products():
            recs = self.db.get_history(p.product_id, days)
            if not recs: continue
            with open(Path("exports")/f"{p.product_id}_{datetime.now():%Y%m%d}.csv","w",newline="") as f:
                w = csv.writer(f); w.writerow(["Timestamp","Price","Currency","Product"])
                for r in recs: w.writerow([r.timestamp.isoformat(),f"{r.price:.2f}",r.currency,p.name])

async def main():
    db = PriceDatabase()
    tracker = PriceTracker(db)
    tracker.add("https://www.amazon.com/dp/B0BSHF7WHW", 299.99)
    tracker.add("https://www.amazon.com/dp/B09V3KXJPB", 899.99)
    await tracker.check()
    tracker.export_csv()
    print(f"Tracking {len(db.get_products())} products")

if __name__ == "__main__":
    asyncio.run(main())
