import { scraperRNF, scraperIdscience, scraperCrossref } from "./pageScraper";

export async function scrape(browserInstance, base, coll) {
  let browser;
  try {
    browser = await browserInstance;
    await scraperRNF.scraper(browser, base, coll);
    await browser.close();
  } catch (err) {
    console.log("Could not resolve the browser instance => ", err);
  }
}

export async function scrape_project(browserInstance, base, coll_in, coll_out) {
  let browser;
  try {
    browser = await browserInstance;
    await scraperRNF.scraper_project(browser, base, coll_in, coll_out);
    await browser.close();
  } catch (err) {
    console.log("Could not resolve the browser instance => ", err);
  }
}

export async function scrape_project_en(
  browserInstance,
  base,
  coll_in,
  coll_out
) {
  let browser;
  try {
    browser = await browserInstance;
    await scraperRNF.scraper_project_en(browser, base, coll_in, coll_out);
    await browser.close();
  } catch (err) {
    console.log("Could not resolve the browser instance => ", err);
  }
}

export async function scrape_orgs(browserInstance, base, coll_in, coll_out) {
  let browser;
  try {
    browser = await browserInstance;
    await scraperIdscience.scraper_orgs(browser, base, coll_in, coll_out);
    await browser.close();
  } catch (err) {
    console.log("Could not resolve the browser instance => ", err);
  }
}

export async function scrape_authors(browserInstance, base, coll_in, coll_out) {
  let browser;
  try {
    browser = await browserInstance;
    await scraperIdscience.scraper_authors(browser, base, coll_in, coll_out);
    await browser.close();
  } catch (err) {
    console.log("Could not resolve the browser instance => ", err);
  }
}

export async function scrape_Allauthors(browserInstance, base, coll_out) {
  let browser;
  try {
    browser = await browserInstance;
    await scraperIdscience.scraper_authors_data(browser, base, coll_out);
    await browser.close();
  } catch (err) {
    console.log("Could not resolve the browser instance => ", err);
  }
}

export async function scrape_Allorgs(browserInstance, base, coll_out) {
  let browser;
  try {
    browser = await browserInstance;
    await scraperIdscience.scraper_orgs_data(browser, base, coll_out);
    await browser.close();
  } catch (err) {
    console.log("Could not resolve the browser instance => ", err);
  }
}

export async function scrape_CrossferJurnalList(
  browserInstance,
  base,
  coll_out
) {
  let browser;
  try {
    browser = await browserInstance;
    await scraperCrossref.jurnal_list(browser, base, coll_out);
    await browser.close();
  } catch (err) {
    console.log("Could not resolve the browser instance => ", err);
  }
}
